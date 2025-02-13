from typing import Any, Dict, List, Optional

from ...model import NodeCreationRule, PropertySet
from ...pipeline.normalizers import LowercaseStrings
from ...pipeline.value_providers import (
    ProviderContext,
    StaticValueOrValueProvider,
    ValueProvider,
)
from ...schema import GraphObjectSchema, SchemaExpansionCoordinator
from .interpretation import Interpretation
from .property_mapping import PropertyMapping

# By default, data gathered from this interpretation is lower cased when a string.
DEFAULT_NORMALIZATION_ARGUMENTS = {LowercaseStrings.argument_flag(): True}


class SourceNodeInterpretation(Interpretation, alias="source_node"):
    """Stores information regarding the source node.

    Within a Pipeline File, a simple usage may look like this:

    ```yaml
    interpretations:
      # Conventionally, this should be first.
      - type: source_node
        node_type: TheTypeOfTheNodeYouAreIngesting
        key:
          id: !!python/jmespath value_for_id_field
    ```

    However, more complex usages may look like this to populate a rich node.

    ```yaml
    interpretations:
      - type: source_node
        node_type: Person
        key:
          first_name: !jmespath first_name
          last_name: !jmespath last_name
        properties:
           bio: !jmespath biography
    ```

    You may also apply additional labels and field level indexes:

    ```yaml
    interpretations:
      - type: source_node
        node_type: Person
        key:
          first_name: !jmespath first_name
          last_name: !jmespath last_name
        properties:
           phone_number: !jmespath phone
        additionally_index:
            - phone_number
        additional_types:
            - Customer
    ```

    You are also allowed by default to send normalization flags in:

    ```yaml
    interpretations:
      - type: source_node
        node_type: DomainName
        key:
          name: !jmespath name
        normalization:
          drop_trailing_dots: true
    ```
    """

    SOURCE_NODE_TYPE_ALIAS = "source_node"
    assigns_source_nodes = True

    __slots__ = (
        "node_type",
        "key",
        "properties",
        "additional_indexes",
        "additional_types",
        "norm_args",
        "allow_create",
        "update_last_ingested",
    )

    def __init__(
        self,
        node_type: StaticValueOrValueProvider,
        key: Dict[str, StaticValueOrValueProvider],
        properties: Optional[Dict[str, StaticValueOrValueProvider]] = None,
        additional_indexes: Optional[List[str]] = None,
        additional_types: Optional[List[str]] = None,
        normalization: Optional[Dict[str, Any]] = None,
        properties_normalization: Optional[Dict[str, Any]] = None,
        key_normalization: Optional[Dict[str, Any]] = None,
        allow_create: bool = True,
        update_last_ingested: bool = True,
    ):
        if normalization and (properties_normalization or key_normalization):
            raise ValueError(
                "You cannot specify normalization both at the root and at the key/properties level."
            )

        properties_normalization = properties_normalization or normalization
        key_normalization = key_normalization or normalization
        self.key_normalization = {
            **DEFAULT_NORMALIZATION_ARGUMENTS,
            **(key_normalization or {}),
        }
        self.properties_normalization = properties_normalization or {}
        self.node_type = ValueProvider.guarantee_value_provider(node_type)
        self.key = PropertyMapping.from_file_data(key or {})
        self.properties = PropertyMapping.from_file_data(properties or {})
        self.additional_indexes = additional_indexes or []
        self.additional_types = tuple(additional_types or [])
        if allow_create:
            self.creation_rule = NodeCreationRule.EAGER
        else:
            self.creation_rule = NodeCreationRule.MATCH_ONLY
        self.update_last_ingested = update_last_ingested

    def interpret(self, context: ProviderContext):
        normalized_key: PropertySet = PropertySet()
        self.key.apply_to(context, normalized_key, self.key_normalization)
        normalized_properties: PropertySet = PropertySet()
        self.properties.apply_to(
            context, normalized_properties, self.properties_normalization
        )

        context.desired_ingest.add_source_node(
            self.node_type.single_value(context),
            self.additional_types,
            self.creation_rule,
            normalized_key,
            normalized_properties,
        )

        if not self.update_last_ingested:
            context.desired_ingest.source.properties.remove_last_ingested()

    def expand_source_node_schema(self, source_node_schema: GraphObjectSchema):
        source_node_schema.add_keys(self.key)
        source_node_schema.add_properties(self.properties)
        source_node_schema.add_indexes(self.additional_indexes)
        source_node_schema.add_indexed_timestamp()

    def expand_schema(self, coordinator: SchemaExpansionCoordinator):
        if not self.node_type.is_static:
            return

        coordinator.on_node_schema(
            self.expand_source_node_schema,
            alias=self.SOURCE_NODE_TYPE_ALIAS,
            node_type=self.node_type.value,
        )
