from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from ..schema.schema import GraphObjectShape, GraphObjectType, KnownTypeMarker

if TYPE_CHECKING:
    from ..databases.ingest_strategy import IngestionStrategy


@dataclass(frozen=True, slots=True)
class TimeToLiveConfiguration:
    graph_object_type: GraphObjectType
    object_type: str
    expiry_in_hours: Optional[int] = 24
    custom_query: Optional[str] = None
    batch_size: int = 100
    enabled: bool = True

    async def ingest(self, strategy: "IngestionStrategy"):
        if self.enabled:
            await strategy.perform_ttl_operation(self)

    def is_for_shape(self, shape: GraphObjectShape):
        return (
            self.graph_object_type == shape.graph_object_type
            and KnownTypeMarker(self.object_type) == shape.object_type
        )
