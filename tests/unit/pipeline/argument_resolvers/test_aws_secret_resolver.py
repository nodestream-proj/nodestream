import time

import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline.argument_resolvers.aws_secret_resolver import (
    AWSSecretResolver,
    SecretCache,
)


@pytest.fixture
def mock_boto3_client(mocker):
    # Reset the singleton instance
    AWSSecretResolver._instance = None
    # Patch boto3 session and client
    mock_client = mocker.Mock()
    mock_session = mocker.patch("boto3.session.Session")
    mock_session.return_value.client.return_value = mock_client
    return mock_client


def test_resolve_string_secret(monkeypatch, mock_boto3_client):
    # Set up environment variable
    monkeypatch.setenv("FAKE_SECRET_ENV", "fake_secret_name")
    # Mock AWS response
    mock_boto3_client.get_secret_value.return_value = {"SecretString": "supersecret"}
    # Should resolve the string secret
    result = AWSSecretResolver.resolve_argument("FAKE_SECRET_ENV")
    assert_that(result, equal_to("supersecret"))


def test_resolve_json_secret(monkeypatch, mock_boto3_client):
    # Set up environment variable
    monkeypatch.setenv("FAKE_JSON_SECRET_ENV", "fake_json_secret_name")
    # Mock AWS response with JSON string
    mock_boto3_client.get_secret_value.return_value = {"SecretString": '{"k": 42}'}
    # Should resolve the JSON secret's key
    result = AWSSecretResolver.resolve_argument("FAKE_JSON_SECRET_ENV.k")
    assert_that(result, equal_to(42))


def test_resolve_json_secret_with_invalid_json_returns_none(
    monkeypatch, mock_boto3_client
):
    # Set up environment variable
    monkeypatch.setenv("FAKE_INVALID_JSON_ENV", "fake_invalid_json_secret_name")
    # Mock AWS response with invalid JSON string
    mock_boto3_client.get_secret_value.return_value = {
        "SecretString": '{"k": 42,}'  # Invalid JSON - trailing comma
    }
    # Should return None when trying to parse invalid JSON
    result = AWSSecretResolver.resolve_argument("FAKE_INVALID_JSON_ENV.k")
    assert_that(result, equal_to(None))


def test_resolve_json_secret_with_malformed_json_returns_none(
    monkeypatch, mock_boto3_client
):
    # Set up environment variable
    monkeypatch.setenv("FAKE_MALFORMED_JSON_ENV", "fake_malformed_json_secret_name")
    # Mock AWS response with malformed JSON string
    mock_boto3_client.get_secret_value.return_value = {
        "SecretString": '{"k": 42'  # Malformed JSON - missing closing brace
    }
    # Should return None when trying to parse malformed JSON
    result = AWSSecretResolver.resolve_argument("FAKE_MALFORMED_JSON_ENV.k")
    assert_that(result, equal_to(None))


def test_secret_cache_hit():
    cache = SecretCache(ttl=5)
    cache.set("foo", "bar")
    assert cache.get("foo") == "bar"


def test_secret_cache_expired(monkeypatch):
    cache = SecretCache(ttl=1)
    cache.set("foo", "bar")
    # Simulate time passing beyond TTL
    original_time = time.time
    monkeypatch.setattr(time, "time", lambda: original_time() + 2)
    assert cache.get("foo") is None
    # After expired, the key should be removed
    assert "foo" not in cache._cache


def test_secret_cache_miss():
    cache = SecretCache(ttl=5)
    assert cache.get("missing") is None


def test_resolve_argument_with_missing_env_var_returns_none(
    monkeypatch, mock_boto3_client
):
    """Test that resolve_argument returns None when environment variable is not set."""
    # Don't set any environment variable
    # Should return None when environment variable is missing
    result = AWSSecretResolver.resolve_argument("MISSING_ENV_VAR")
    assert_that(
        result,
        equal_to(None),
    )


def test_resolve_argument_with_empty_env_var_returns_none(
    monkeypatch, mock_boto3_client
):
    """Test that resolve_argument returns None when environment variable is empty."""
    # Set environment variable to empty string
    monkeypatch.setenv("EMPTY_ENV_VAR", "")
    # Should return None when environment variable is empty
    result = AWSSecretResolver.resolve_argument("EMPTY_ENV_VAR")
    assert_that(
        result,
        equal_to(None),
    )


def test_resolve_json_argument_with_missing_env_var_returns_none(
    monkeypatch, mock_boto3_client
):
    """Test that resolve_argument returns None for JSON secrets when env var is missing."""
    # Don't set any environment variable
    # Should return None when environment variable is missing
    result = AWSSecretResolver.resolve_argument("MISSING_ENV_VAR.key")
    assert_that(
        result,
        equal_to(None),
    )


def test_resolve_argument_with_unexpected_exception_returns_none(
    monkeypatch, mock_boto3_client
):
    """Test that resolve_argument returns None when an unexpected exception occurs."""
    # Clear the cache to ensure we don't get cached results
    from nodestream.pipeline.argument_resolvers.aws_secret_resolver import (
        _get_secret_cache,
    )

    _get_secret_cache()._cache.clear()

    # Set up environment variable
    monkeypatch.setenv("FAKE_SECRET_ENV", "fake_secret_name")
    # Mock AWS client to raise an unexpected exception
    mock_boto3_client.get_secret_value.side_effect = Exception("Unexpected error")

    # Should return None when an unexpected exception occurs
    result = AWSSecretResolver.resolve_argument("FAKE_SECRET_ENV")
    assert_that(result, equal_to(None))
