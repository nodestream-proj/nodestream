from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from logging import getLogger
from threading import current_thread
from typing import Any, Dict, List, Optional, Tuple

LOGGER = getLogger(__name__)


JsonDocument = Dict[str, Any]


class MatchStrategy(str, Enum):
    EAGER = "EAGER"
    MATCH_ONLY = "MATCH_ONLY"
    FUZZY = "FUZZY"


class GraphObjectType(str, Enum):
    RELATIONSHIP = "RELATIONSHIP"
    NODE = "NODE"

    def __str__(self) -> str:
        return self.value


def default_metadata():
    """Generates default metadata containing of time and pipeline name."""
    pipeline_name = current_thread().name
    now = datetime.utcnow()
    return Metadata(
        {
            "last_ingested_at": now,
            f"last_ingested_by_{pipeline_name}_at": now,
            f"was_ingested_by_{pipeline_name}": True,
        }
    )


def default_var():
    """Generates default variables"""
    return GlobalVariable(dict())


def default_related_metadata():
    pipeline_name = current_thread().name
    now = datetime.utcnow()
    return Metadata(
        {"last_ingested_at": now, f"last_linked_from_{pipeline_name}_at": now}
    )


class IngestionHook(ABC):
    """An IngestionHook is a custom piece of logic that is bundled as a query.

    IngestionHooks provide a mechanism to enrich the graph model that is derived from
    data added to and currently existing in the graph. For example, drawing an edge
    between two nodes where following a complex path.
    """

    @abstractmethod
    def as_cypher_query_and_parameters(self) -> Tuple[str, Dict[str, Any]]:
        """Returns a cypher query string and parameters to execute."""

        raise NotImplementedError


class Metadata(dict):
    """Used by `SourceNode` and `Relationship`. Provides a store for `metadata`."""

    def add_metadata(self, key: str, value: Any):
        """Put a new key,value pair into the metadata of the object.

        Overrides the value if already present.
        """
        self[key] = value


class GlobalVariable(dict):
    """Used by `SourceNode` and `Relationship`. Provides a store for `global_var`."""

    def add_global_var(self, key: str, value: Any):
        """Put a new key,value pair into the global_var of the object.

        Overrides the value if already present.
        """
        self[key] = value


@dataclass
class SourceNode:
    """A `SourceNode` is the center of ingestion.

    Its the node that relationships are drawn to or from depending on the
    direction of the relationship.
    """

    type: str
    identity_values: Dict[str, Any]
    metadata: Metadata = field(default_factory=default_metadata)
    additional_types: Tuple[str] = field(default_factory=tuple)


@dataclass
class Relationship:
    """Stores information about the related node and the relationship itself."""

    related_node_type: str
    relationship_type: str
    related_node_identity_field_value: str
    related_node_identity_field_name: str = "id"
    metadata: Metadata = field(default_factory=default_metadata)
    related_metadata: Metadata = field(default_factory=default_related_metadata)
    relationship_identity: Dict[str, Any] = field(default_factory=dict)

    match_strategy: MatchStrategy = MatchStrategy.EAGER

    def relation_identity(self) -> Dict[str, Any]:
        """The identity (primary key values) for the node that the relationship is connecting to.

        Returns:
            The identity of the relationship as key,value pairs.
        """
        return {
            self.related_node_identity_field_name: self.related_node_identity_field_value.lower()
        }


class IngestionStrategy(ABC):
    @abstractmethod
    def ingest_source_node(self, source: SourceNode):
        raise NotImplementedError

    @abstractmethod
    def ingest_inbound_relationship(self, source: SourceNode, inbound: Relationship):
        raise NotImplementedError

    @abstractmethod
    def ingest_outbound_relationship(self, source: SourceNode, outbound: Relationship):
        raise NotImplementedError

    @abstractmethod
    def run_hook(self, hook: IngestionHook, run_before_ingest: bool):
        raise NotImplementedError

    @abstractmethod
    def upsert_key_index(self, index: "KeyIndex"):
        raise NotImplementedError

    @abstractmethod
    def upsert_field_index(self, index: "FieldIndex"):
        raise NotImplementedError

    @abstractmethod
    def perform_ttl_operation(self, config: "TimeToLiveConfiguration"):
        raise NotImplementedError


@dataclass(frozen=True)
class KeyIndex:
    type: str
    identity_keys: frozenset

    def ingest(self, strategy: IngestionStrategy):
        strategy.upsert_key_index(self)


@dataclass(frozen=True)
class FieldIndex:
    type: str
    field: str
    object_type: GraphObjectType

    def ingest(self, strategy: IngestionStrategy):
        strategy.upsert_field_index(self)

    @classmethod
    def for_ttl_timestamp(
        cls, type, object_type: GraphObjectType = GraphObjectType.NODE
    ):
        return cls(type, "last_ingested_at", object_type=object_type)


@dataclass
class DesiredIngestion:
    source: SourceNode = None
    global_var: GlobalVariable = field(default_factory=default_var)
    inbound_connections: List[Relationship] = field(default_factory=list)
    outbound_connections: List[Relationship] = field(default_factory=list)
    before_ingest_hooks: List[IngestionHook] = field(default_factory=list)
    after_ingest_hooks: List[IngestionHook] = field(default_factory=list)

    @property
    def source_node_is_valid(self) -> bool:
        # This may need to change to handle null values for multi-key ingest nodes
        if self.source is None:
            return False
        return not all(value is None for value in self.source.identity_values.values())

    @property
    def total_relationships(self) -> int:
        return len(self.inbound_connections) + len(self.outbound_connections)

    def ingest_source_node(self, strategy: IngestionStrategy):
        strategy.ingest_source_node(self.source)

    def ingest_inbound_relationships(self, strategy: IngestionStrategy):
        for inbound_connection in self.inbound_connections:
            strategy.ingest_inbound_relationship(self.source, inbound_connection)

    def ingest_outbound_relationships(self, strategy: IngestionStrategy):
        for outbound_connection in self.outbound_connections:
            strategy.ingest_outbound_relationship(self.source, outbound_connection)

    def run_ingest_hooks(self, strategy: IngestionStrategy):
        for hook in self.before_ingest_hooks:
            strategy.run_hook(hook, run_before_ingest=True)
        for hook in self.after_ingest_hooks:
            strategy.run_hook(hook, run_before_ingest=False)

    def can_perform_ingest(self):
        # We can do the main part of the ingest if the source node is valid.
        # If its not valid, its only an error when there are realtionships we are
        # trying to ingest as well.
        if not self.source_node_is_valid:
            if self.total_relationships > 0:
                LOGGER.warning(
                    "Identity value for source node was null. Skipping Ingest.",
                    extra=asdict(self),
                )
            else:
                LOGGER.debug(
                    "Ingest was not provided a valid source node and no relationships. Only running ingest hooks.",
                    extra=asdict(self),
                )
            return False
        return True

    def ingest(self, strategy: IngestionStrategy):
        if self.can_perform_ingest():
            self.ingest_source_node(strategy)
            self.ingest_inbound_relationships(strategy)
            self.ingest_outbound_relationships(strategy)
        self.run_ingest_hooks(strategy)

    def create_inbound_relationship(self, relationship: Relationship):
        self.inbound_connections.append(relationship)

    def create_outbound_relationship(self, relationship: Relationship):
        self.outbound_connections.append(relationship)

    def add_ingest_hook(self, ingest_hook: IngestionHook, run_before_ingest=False):
        if run_before_ingest:
            self.before_ingest_hooks.append(ingest_hook)
        else:
            self.after_ingest_hooks.append(ingest_hook)


@dataclass
class IngestContext:
    document: JsonDocument
    desired_ingest: DesiredIngestion
    mappings: Any = None


@dataclass
class TimeToLiveConfiguration:
    graph_type: GraphObjectType
    object_type: str
    expiry_in_hours: Optional[int] = 24
    custom_query: Optional[str] = None
    batch_size: int = 100
    enabled: bool = True

    def ingest(self, strategy: IngestionStrategy):
        if self.enabled:
            strategy.perform_ttl_operation(self)
