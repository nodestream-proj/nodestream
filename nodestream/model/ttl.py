from dataclasses import dataclass
from typing import Optional

from .ingest_strategy import IngestionStrategy
from .schema import GraphObjectType


@dataclass(frozen=True, slots=True)
class TimeToLiveConfiguration:
    graph_type: GraphObjectType
    object_type: str
    expiry_in_hours: Optional[int] = 24
    custom_query: Optional[str] = None
    batch_size: int = 100
    enabled: bool = True

    async def ingest(self, strategy: "IngestionStrategy"):
        if self.enabled:
            await strategy.perform_ttl_operation(self)
