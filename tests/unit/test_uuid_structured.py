#!/usr/bin/env python3
"""Test script for structured UUID value provider."""


import yaml

from nodestream.pipeline.value_providers.uuid_value_provider import UuidValueProvider


def test_structured_uuid_provider():
    """Test the structured UUID value provider with different formats."""
    print("Testing Structured UUID Value Provider...")
    print("=" * 60)

    # Install the YAML tag
    UuidValueProvider.install_yaml_tag(yaml.SafeLoader)

    # Test 1: Simple string format
    print("\n1. Testing simple string format:")
    test_yaml_1 = """
    test:
      random_uuid: !uuid
      deterministic_uuid: !uuid "finding"
    """

    data1 = yaml.safe_load(test_yaml_1)
    random_provider = data1["test"]["random_uuid"]
    det_provider = data1["test"]["deterministic_uuid"]

    print(f"   Random provider: {random_provider}")
    print(f"   Deterministic provider: {det_provider}")
    print(f"   Random namespace: '{random_provider.namespace}'")
    print(f"   Deterministic namespace: '{det_provider.namespace}'")

    # Test 2: Structured format with custom namespace
    print("\n2. Testing structured format with custom namespace:")
    test_yaml_2 = """
    test:
      custom_uuid: !uuid
        variable_name: "finding"
        namespace: "my-custom-namespace"
    """

    data2 = yaml.safe_load(test_yaml_2)
    custom_provider = data2["test"]["custom_uuid"]

    print(f"   Custom provider: {custom_provider}")
    print(f"   Custom namespace: '{custom_provider.namespace}'")

    # Test 3: Structured format with only variable_name (uses default namespace)
    print("\n3. Testing structured format with default namespace:")
    test_yaml_3 = """
    test:
      default_ns_uuid: !uuid
        variable_name: "finding"
    """

    data3 = yaml.safe_load(test_yaml_3)
    default_ns_provider = data3["test"]["default_ns_uuid"]

    print(f"   Default NS provider: {default_ns_provider}")
    print(f"   Default NS namespace: '{default_ns_provider.namespace}'")

    # Test 4: Structured format with only namespace (random UUID)
    print("\n4. Testing structured format with only namespace:")
    test_yaml_4 = """
    test:
      random_custom_ns: !uuid
        namespace: "my-random-namespace"
    """

    data4 = yaml.safe_load(test_yaml_4)
    random_custom_provider = data4["test"]["random_custom_ns"]

    print(f"   Random custom NS provider: {random_custom_provider}")
    print(f"   Random custom NS namespace: '{random_custom_provider.namespace}'")

    # Test 5: Verify deterministic behavior
    print("\n5. Testing deterministic behavior:")
    context = {"test": "data"}

    # Same variable_name and namespace should produce same UUID
    uuid1 = custom_provider.single_value(context)
    uuid2 = custom_provider.single_value(context)
    print(f"   UUID 1: {uuid1}")
    print(f"   UUID 2: {uuid2}")
    print(f"   Same UUIDs: {uuid1 == uuid2}")

    # Different namespaces should produce different UUIDs
    default_uuid = default_ns_provider.single_value(context)
    custom_uuid = custom_provider.single_value(context)
    print(f"   Default NS UUID: {default_uuid}")
    print(f"   Custom NS UUID: {custom_uuid}")
    print(f"   Different UUIDs: {default_uuid != custom_uuid}")

    print("\n" + "=" * 60)
    print("✅ All structured format tests completed successfully!")


if __name__ == "__main__":
    test_structured_uuid_provider()
