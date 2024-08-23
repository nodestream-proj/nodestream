from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from ..schema import GraphObjectSchema, GraphObjectType

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

    def applies_to(
        self, graph_object_type: GraphObjectType, type_def: GraphObjectSchema
    ):
        return (
            graph_object_type == self.graph_object_type
            and type_def.name == self.object_type
        )
