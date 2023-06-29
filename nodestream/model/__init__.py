from typing import Dict, Any

from .desired_ingestion import DesiredIngestion, RelationshipWithNodes
from .graph_objects import (
    Node,
    NodeIdentityShape,
    PropertySet,
    Relationship,
    RelationshipIdentityShape,
)
from .match_strategy import MatchStrategy
from .ingestion_hooks import IngestionHook, IngestionHookRunRequest

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
