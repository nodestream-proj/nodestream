from typing import Any, Dict, Iterable, Optional

from ...pipeline.value_providers import (
    ProviderContext,
    StaticValueOrValueProvider,
    ValueProvider,
)
from ...schema.schema import (
    GraphObjectShape,
    GraphObjectType,
    PropertyMetadataSet,
    UnknownTypeMarker,
)
from .interpretation import Interpretation


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
        source.properties.apply_providers(context, self.properties, **self.norm_args)

    def gather_object_shapes(self) -> Iterable[GraphObjectShape]:
        yield GraphObjectShape(
            graph_object_type=GraphObjectType.NODE,
            object_type=UnknownTypeMarker.source_node(),
            properties=PropertyMetadataSet.from_names(self.properties.keys()),
        )
