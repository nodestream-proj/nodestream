from typing import Any, Dict

from .desired_ingest import DesiredIngestion, MatchStrategy, RelationshipWithNodes
from .graph_objects import Node, PropertySet, Relationship
from .indexes import FieldIndex, KeyIndex
from .ingest_strategy import IngestionStrategy
from .ingestion_hooks import IngestionHook, IngestionHookRunRequest
from .interpreter_context import InterpreterContext, JsonLikeDocument
from .record_decomposers import RecordDecomposer
from .ttl import TimeToLiveConfiguration
from .value_provider import ValueProvider, StaticValueOrValueProvider
from .schema import (
    GraphObjectShape,
    GraphObjectType,
    TypeMarker,
    KnownTypeMarker,
    UnknownTypeMarker,
    PropertyMetadataSet,
    PropertyMetadata,
    PropertyType,
    IntrospectableIngestionComponent,
    AggregatedIntrospectionMixin,
)

__all__ = (
    "DesiredIngestion",
    "RelationshipWithNodes",
    "MatchStrategy",
    "PropertySet",
    "Node",
    "Relationship",
    "KeyIndex",
    "FieldIndex",
    "IngestionStrategy",
    "IngestionHook",
    "IngestionHookRunRequest",
    "TimeToLiveConfiguration",
    "ValueProvider",
    "InterpreterContext",
    "RecordDecomposer",
    "JsonLikeDocument",
    "GraphObjectShape",
    "GraphObjectType",
    "TypeMarker",
    "KnownTypeMarker",
    "UnknownTypeMarker",
    "PropertyMetadataSet",
    "PropertyMetadata",
    "PropertyType",
    "IntrospectableIngestionComponent",
    "AggregatedIntrospectionMixin",
    "StaticValueOrValueProvider",
)
