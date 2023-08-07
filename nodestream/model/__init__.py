from typing import Any, Dict

from .desired_ingestion import DesiredIngestion, RelationshipWithNodes
from .graph_objects import (
    Node,
    NodeIdentityShape,
    PropertySet,
    Relationship,
    RelationshipIdentityShape,
)
from .ingestion_hooks import IngestionHook, IngestionHookRunRequest
from .match_strategy import MatchStrategy
from .ttl import TimeToLiveConfiguration

JsonLikeDocument = Dict[str, Any]


__all__ = (
    "DesiredIngestion",
    "RelationshipWithNodes",
    "Node",
    "NodeIdentityShape",
    "PropertySet",
    "Relationship",
    "RelationshipIdentityShape",
    "MatchStrategy",
    "IngestionHook",
    "IngestionHookRunRequest",
    "TimeToLiveConfiguration",
    "JsonLikeDocument",
)
