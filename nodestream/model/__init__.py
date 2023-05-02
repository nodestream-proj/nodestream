from typing import Dict, Any

from .desired_ingest import DesiredIngestion, RelationshipWithNodes, MatchStrategy
from .graph_objects import PropertySet, Node, Relationship
from .indexes import KeyIndex, FieldIndex
from .ingest_strategy import IngestionStrategy
from .ingestion_hooks import IngestionHook, IngestionHookRunRequest
from .ttl import TimeToLiveConfiguration

JsonDocument = Dict[str, Any]


__all__ = (
    "DesiredIngestion", "RelationshipWithNodes", "MatchStrategy",
    "PropertySet", "Node", "Relationship", "KeyIndex", "FieldIndex",
    "IngestionStrategy", "IngestionHook", "IngestionHookRunRequest",
    "TimeToLiveConfiguration"
)