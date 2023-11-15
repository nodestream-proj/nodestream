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
from .creation_rules import NodeCreationRule, RelationshipCreationRule
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
    "NodeCreationRule",
    "IngestionHook",
    "IngestionHookRunRequest",
    "TimeToLiveConfiguration",
    "JsonLikeDocument",
    "RelationshipCreationRule",
)
