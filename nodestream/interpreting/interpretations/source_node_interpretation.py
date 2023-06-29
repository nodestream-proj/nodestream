from typing import Any, Dict, Iterable, List, Optional, Union

from ...pipeline.normalizers import LowercaseStrings
from ...pipeline.value_providers import (
    ProviderContext,
    StaticValueOrValueProvider,
    ValueProvider,
)
from ...schema.indexes import FieldIndex, KeyIndex
from ...schema.schema import (
    GraphObjectShape,
    GraphObjectType,
    KnownTypeMarker,
    PropertyMetadataSet,
)
from .interpretation import Interpretation

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

    __slots__ = (
        "node_type",
        "key",
        "properties",
        "additional_indexes",
        "additional_types",
        "norm_args",
    )

    def __init__(
        self,
        node_type: StaticValueOrValueProvider,
        key: Dict[str, StaticValueOrValueProvider],
        properties: Optional[Dict[str, StaticValueOrValueProvider]] = None,
        additional_indexes: Optional[List[str]] = None,
        additional_types: Optional[List[str]] = None,
        normalization: Optional[Dict[str, Any]] = None,
    ):
        self.node_type = ValueProvider.guarantee_value_provider(node_type)
        self.key = ValueProvider.guarantee_provider_dictionary(key)
        self.properties = ValueProvider.guarantee_provider_dictionary(properties or {})
        self.additional_indexes = additional_indexes or []
        self.additional_types = tuple(additional_types or [])
        self.norm_args = {**DEFAULT_NORMALIZATION_ARGUMENTS, **(normalization or {})}

    def interpret(self, context: ProviderContext):
        source = context.desired_ingest.source
        source.type = self.node_type.single_value(context)
        source.key_values.apply_providers(context, self.key, **self.norm_args)
        source.properties.apply_providers(context, self.properties, **self.norm_args)
        source.additional_types = self.additional_types

    def gather_used_indexes(self) -> Iterable[Union[KeyIndex, FieldIndex]]:
        # NOTE: We cannot generate indexes when we do not know the type until runtime.
        if not self.node_type.is_static:
            return

        node_type = self.node_type.value
        yield KeyIndex(node_type, frozenset(self.key.keys()))
        yield FieldIndex.for_ttl_timestamp(node_type)
        for field in self.additional_indexes:
            yield FieldIndex(node_type, field, object_type=GraphObjectType.NODE)

    def gather_object_shapes(self) -> Iterable[GraphObjectShape]:
        # NOTE: We cannot generate schemas when we do not know the type until runtime.
        if not self.node_type.is_static:
            return

        node_type = self.node_type.value
        yield GraphObjectShape(
            graph_object_type=GraphObjectType.NODE,
            object_type=KnownTypeMarker.fulfilling_source_node(node_type),
            properties=PropertyMetadataSet.from_names(
                self.key.keys(), self.properties.keys()
            ),
        )
