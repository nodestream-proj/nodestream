"""Test script for structured UUID value provider."""

import uuid
from unittest.mock import patch

import pytest
import yaml
from hamcrest import assert_that, equal_to, is_not, matches_regexp

from nodestream.pipeline.value_providers.uuid_value_provider import UuidValueProvider


@pytest.fixture
def yaml_loader():
    """Set up YAML loader with UUID tag."""
    UuidValueProvider.install_yaml_tag(yaml.SafeLoader)
    return yaml.SafeLoader


def test_simple_string_format_parsing(yaml_loader):
    """Test that simple string format is parsed correctly."""
    test_yaml = """
    test:
      random_uuid: !uuid
      deterministic_uuid: !uuid "finding"
    """

    data = yaml.safe_load(test_yaml)
    random_provider = data["test"]["random_uuid"]
    det_provider = data["test"]["deterministic_uuid"]

    # Test random UUID provider
    assert_that(random_provider.variable_name, equal_to(""))
    assert_that(random_provider.namespace, equal_to("nodestream"))

    # Test deterministic UUID provider
    assert_that(det_provider.variable_name, equal_to("finding"))
    assert_that(det_provider.namespace, equal_to("nodestream"))


def test_structured_format_with_custom_namespace(yaml_loader):
    """Test structured format with custom namespace."""
    test_yaml = """
    test:
      custom_uuid: !uuid
        variable_name: "finding"
        namespace: "my-custom-namespace"
    """

    data = yaml.safe_load(test_yaml)
    custom_provider = data["test"]["custom_uuid"]

    assert_that(custom_provider.variable_name, equal_to("finding"))
    assert_that(custom_provider.namespace, equal_to("my-custom-namespace"))


def test_structured_format_with_default_namespace(yaml_loader):
    """Test structured format with only variable_name (uses default namespace)."""
    test_yaml = """
    test:
      default_ns_uuid: !uuid
        variable_name: "finding"
    """

    data = yaml.safe_load(test_yaml)
    default_ns_provider = data["test"]["default_ns_uuid"]

    assert_that(default_ns_provider.variable_name, equal_to("finding"))
    assert_that(default_ns_provider.namespace, equal_to("nodestream"))


def test_structured_format_with_only_namespace(yaml_loader):
    """Test structured format with only namespace (random UUID)."""
    test_yaml = """
    test:
      random_custom_ns: !uuid
        namespace: "my-random-namespace"
    """

    data = yaml.safe_load(test_yaml)
    random_custom_provider = data["test"]["random_custom_ns"]

    assert_that(random_custom_provider.variable_name, equal_to(""))
    assert_that(random_custom_provider.namespace, equal_to("my-random-namespace"))


def test_deterministic_uuid_consistency():
    """Test that deterministic UUIDs are consistent for same inputs."""
    provider = UuidValueProvider(variable_name="finding", namespace="test-namespace")
    context = {"test": "data"}

    # Same inputs should produce same UUID
    uuid1 = provider.single_value(context)
    uuid2 = provider.single_value(context)

    assert_that(uuid1, equal_to(uuid2))
    assert_that(
        uuid1,
        matches_regexp(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        ),
    )


def test_different_namespaces_produce_different_uuids():
    """Test that different namespaces produce different UUIDs."""
    provider1 = UuidValueProvider(variable_name="finding", namespace="namespace1")
    provider2 = UuidValueProvider(variable_name="finding", namespace="namespace2")
    context = {"test": "data"}

    uuid1 = provider1.single_value(context)
    uuid2 = provider2.single_value(context)

    assert_that(uuid1, is_not(equal_to(uuid2)))


def test_random_uuid_generation():
    """Test that random UUIDs are generated correctly."""
    provider = UuidValueProvider()  # No variable_name = random UUID
    context = {"test": "data"}

    uuid1 = provider.single_value(context)
    uuid2 = provider.single_value(context)

    # Random UUIDs should be different
    assert_that(uuid1, is_not(equal_to(uuid2)))
    # Should be valid UUID format
    assert_that(
        uuid1,
        matches_regexp(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        ),
    )
    assert_that(
        uuid2,
        matches_regexp(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        ),
    )


def test_random_uuid_with_custom_namespace():
    """Test random UUID generation with custom namespace."""
    provider = UuidValueProvider(namespace="custom-namespace")  # No variable_name
    context = {"test": "data"}

    uuid1 = provider.single_value(context)
    uuid2 = provider.single_value(context)

    # Should be different (random)
    assert_that(uuid1, is_not(equal_to(uuid2)))
    # Should be valid UUID format
    assert_that(
        uuid1,
        matches_regexp(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        ),
    )


def test_uuid_generation_with_patched_uuid():
    """Test UUID generation with patched uuid.uuid4 for deterministic testing."""
    with patch("uuid.uuid4") as mock_uuid4:
        mock_uuid4.return_value = uuid.UUID("12345678-1234-5678-9abc-def123456789")

        provider = UuidValueProvider()  # Random UUID
        context = {"test": "data"}

        result = provider.single_value(context)

        assert_that(result, equal_to("12345678-1234-5678-9abc-def123456789"))
        mock_uuid4.assert_called_once()


def test_deterministic_uuid_with_patched_uuid5():
    """Test deterministic UUID generation with patched uuid.uuid5."""
    with patch("uuid.uuid5") as mock_uuid5:
        mock_uuid5.return_value = uuid.UUID("87654321-4321-8765-cba9-fed876543210")

        provider = UuidValueProvider(variable_name="test", namespace="test-namespace")
        context = {"test": "data"}

        result = provider.single_value(context)

        assert_that(result, equal_to("87654321-4321-8765-cba9-fed876543210"))
        # Should be called twice: once for namespace UUID, once for final UUID
        assert_that(mock_uuid5.call_count, equal_to(2))


def test_empty_variable_name_handling():
    """Test handling of empty variable_name."""
    provider = UuidValueProvider(variable_name="", namespace="test-namespace")
    context = {"test": "data"}

    uuid1 = provider.single_value(context)
    uuid2 = provider.single_value(context)

    # Should generate random UUIDs (different each time)
    assert_that(uuid1, is_not(equal_to(uuid2)))


def test_whitespace_handling():
    """Test that whitespace is properly stripped."""
    provider = UuidValueProvider(variable_name="  test  ", namespace="  namespace  ")

    assert_that(provider.variable_name, equal_to("test"))
    assert_that(provider.namespace, equal_to("namespace"))


def test_empty_namespace_uses_default():
    """Test that empty namespace uses default."""
    provider = UuidValueProvider(variable_name="test", namespace="")

    assert_that(provider.namespace, equal_to("nodestream"))


def test_many_values_generator():
    """Test that many_values yields the same value as single_value."""
    provider = UuidValueProvider(variable_name="test")
    context = {"test": "data"}

    single_result = provider.single_value(context)
    many_results = list(provider.many_values(context))

    assert_that(len(many_results), equal_to(1))
    assert_that(many_results[0], equal_to(single_result))


def test_string_representation():
    """Test the string representation of the provider."""
    provider = UuidValueProvider(variable_name="test", namespace="test-namespace")

    expected = (
        "UuidValueProvider: {'variable_name': 'test', 'namespace': 'test-namespace'}"
    )
    assert_that(str(provider), equal_to(expected))


def test_empty_yaml_format(yaml_loader):
    """Test empty YAML format: !uuid (no value)."""
    test_yaml = """
    test:
      empty_uuid: !uuid
    """

    data = yaml.safe_load(test_yaml)
    empty_provider = data["test"]["empty_uuid"]

    assert_that(empty_provider.variable_name, equal_to(""))
    assert_that(empty_provider.namespace, equal_to("nodestream"))


def test_exception_handling_in_uuid_generation():
    """Test that exceptions during UUID generation are handled gracefully."""
    # Test random UUID with exception in uuid4, fallback should succeed
    with patch(
        "uuid.uuid4",
        side_effect=[
            Exception("UUID generation failed"),
            uuid.UUID("12345678-1234-5678-9abc-def123456789"),
        ],
    ):
        provider1 = UuidValueProvider()  # Random UUID
        context = {"test": "data"}
        result1 = provider1.single_value(context)
        assert_that(result1, equal_to("12345678-1234-5678-9abc-def123456789"))

    # Test deterministic UUID with exception in uuid5, fallback should succeed
    with patch("uuid.uuid5", side_effect=Exception("UUID generation failed")):
        provider2 = UuidValueProvider(variable_name="test", namespace="test-namespace")
        context = {"test": "data"}
        # The fallback will use uuid.uuid4, which is not patched here
        result2 = provider2.single_value(context)
        assert_that(
            result2,
            matches_regexp(
                r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
            ),
        )
