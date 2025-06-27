import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline.argument_resolvers.aws_secret_resolver import (
    AWSSecretResolver,
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


def test_resolve_string_secret(monkeypatch, mocker, mock_boto3_client):
    # Set up environment variable
    monkeypatch.setenv("FAKE_SECRET_ENV", "fake_secret_name")
    # Mock AWS response
    mock_boto3_client.get_secret_value.return_value = {"SecretString": "supersecret"}
    # Should resolve the string secret
    result = AWSSecretResolver.resolve_argument("FAKE_SECRET_ENV")
    assert_that(result, equal_to("supersecret"))


def test_resolve_json_secret(monkeypatch, mocker, mock_boto3_client):
    # Set up environment variable
    monkeypatch.setenv("FAKE_JSON_SECRET_ENV", "fake_json_secret_name")
    # Mock AWS response with JSON string
    mock_boto3_client.get_secret_value.return_value = {"SecretString": '{"k": 42}'}
    # Should resolve the JSON secret's key
    result = AWSSecretResolver.resolve_argument("FAKE_JSON_SECRET_ENV.k")
    assert_that(result, equal_to(42))
