from typing import Any, Dict, Optional

from ...pipeline.value_providers import (
    ProviderContext,
    StaticValueOrValueProvider,
    ValueProvider,
)
from ...schema import GraphObjectSchema, SchemaExpansionCoordinator
from .interpretation import Interpretation
from .source_node_interpretation import SourceNodeInterpretation


class PropertiesInterpretation(Interpretation, alias="properties"):
    """Stores additional properties onto the source node."""

    __slots__ = ("properties", "norm_args")

    def __init__(
        self,
        properties: StaticValueOrValueProvider,
        normalization: Optional[Dict[str, Any]] = None,
    ):
        self.properties = ValueProvider.guarantee_provider_dictionary(properties)
        self.norm_args = normalization or {}

    def interpret(self, context: ProviderContext):
        source = context.desired_ingest.source
        source.properties.apply_providers(context, self.properties, self.norm_args)

    def expand_schema(self, coordinator: SchemaExpansionCoordinator):
        coordinator.on_node_schema(
            self.expand_source_node_schema,
            alias=SourceNodeInterpretation.SOURCE_NODE_TYPE_ALIAS,
        )

    def expand_source_node_schema(self, schema: GraphObjectSchema):
        schema.add_properties(self.properties)
