from __future__ import annotations

import logging
import uuid
from typing import Any, Type

from yaml import SafeDumper, SafeLoader

from .context import ProviderContext
from .value_provider import ValueProvider

# Configure structured logging
logger = logging.getLogger(__name__)

# Default namespace for our application
DEFAULT_NAMESPACE = "nodestream"


class UuidValueProvider(ValueProvider):
    """UUID generator value provider for Nodestream.

    This value provider generates UUIDs on demand and can be used with the !uuid
    tag in Nodestream configuration files.

    Supports both simple string input and structured configuration:

    Simple format:
        id: !uuid  # Random UUID v4
        id: !uuid "finding"  # Deterministic UUID v5 based on "finding"

    Structured format:
        # Full configuration with both variable_name and namespace
        id: !uuid
          variable_name: "finding"
          namespace: "my-custom-namespace"

        # Only variable_name (uses default namespace "nodestream")
        id: !uuid
          variable_name: "exposure_finding"

        # Only namespace (generates random UUID v4 with custom namespace)
        id: !uuid
          namespace: "my-random-namespace"

        # Empty configuration (generates random UUID v4 with default namespace)
        id: !uuid

    When a variable_name is provided, it generates a deterministic UUID v5
    based on the namespace and variable_name. When no variable_name is provided,
    it generates a random UUID v4.
    """

    __slots__ = ("variable_name", "namespace")

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!uuid",
            lambda loader, node: cls.from_yaml_node(loader, node),
        )

    @classmethod
    def from_yaml_node(cls, loader: SafeLoader, node):
        """Create a UuidValueProvider from YAML node."""
        if node.id == "scalar":
            # Simple string format: !uuid "finding"
            expression = loader.construct_scalar(node)
            return cls.from_string_expression(expression)
        elif node.id == "mapping":
            # Structured format: !uuid { variable_name: "finding", namespace: "ns" }
            data = loader.construct_mapping(node)
            return cls.from_structured_data(data)
        else:
            # Empty format: !uuid
            return cls()

    @classmethod
    def from_string_expression(cls, expression: str):
        """Create from simple string expression."""
        return cls(variable_name=expression)

    @classmethod
    def from_structured_data(cls, data: dict):
        """Create from structured data dictionary."""
        variable_name = data.get("variable_name", "")
        namespace = data.get("namespace", DEFAULT_NAMESPACE)
        return cls(variable_name=variable_name, namespace=namespace)

    def __init__(self, variable_name: str = "", namespace: str = DEFAULT_NAMESPACE):
        """Initialize the UUID value provider.

        Args:
            variable_name: If provided, generates a deterministic UUID v5
                          based on this name. If empty, generates a random UUID v4.
            namespace: The namespace to use for deterministic UUID generation.
                      Defaults to "nodestream".
        """
        self.variable_name = variable_name.strip() if variable_name else ""
        self.namespace = namespace.strip() if namespace else DEFAULT_NAMESPACE

    def single_value(self, context: ProviderContext) -> Any:
        """Generate a UUID value.

        Args:
            context: The provider context containing record data

        Returns:
            A new UUID string

        Example:
            In nodestream.yaml:
                id: !uuid  # Random UUID v4
                id: !uuid "finding"  # Deterministic UUID v5
        """
        try:
            if self.variable_name:
                # Generate namespace UUID from the string
                namespace_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, self.namespace)
                # Generate deterministic UUID v5 based on variable_name
                new_uuid = str(uuid.uuid5(namespace_uuid, self.variable_name))
                logger.debug(
                    f"Generated deterministic UUID v5: {new_uuid[:8]}... "
                    f"(namespace: '{self.namespace}', variable: '{self.variable_name}')"
                )
            else:
                # Generate random UUID v4
                new_uuid = str(uuid.uuid4())
                logger.debug(f"Generated random UUID v4: {new_uuid[:8]}...")

            return new_uuid
        except Exception as e:
            logger.error(f"Error generating UUID: {e}", exc_info=True)
            # Return a fallback UUID in case of error
            return str(uuid.uuid4())

    def many_values(self, context: ProviderContext):
        yield self.single_value(context)

    def __str__(self) -> str:
        return f"UuidValueProvider: {{'variable_name': '{self.variable_name}', 'namespace': '{self.namespace}'}}"


SafeDumper.add_representer(
    UuidValueProvider,
    lambda dumper, uuid_provider: dumper.represent_scalar(
        "!uuid", uuid_provider.variable_name
    ),
)
