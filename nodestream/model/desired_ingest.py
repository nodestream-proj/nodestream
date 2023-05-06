from dataclasses import asdict, dataclass, field
from enum import Enum
from logging import getLogger
from typing import List

from .graph_objects import Node, Relationship
from .ingest_strategy import IngestionStrategy
from .ingestion_hooks import IngestionHook, IngestionHookRunRequest

LOGGER = getLogger(__name__)


class MatchStrategy(str, Enum):
    EAGER = "EAGER"
    MATCH_ONLY = "MATCH_ONLY"
    FUZZY = "FUZZY"


@dataclass(slots=True)
class RelationshipWithNodes:
    """Stores information about the related node and the relationship itself."""

    from_node: Node
    to_node: Node
    relationship: Relationship
    match_strategy: MatchStrategy = MatchStrategy.EAGER


@dataclass(slots=True)
class DesiredIngestion:
    source: Node = field(default_factory=Node)
    relationships: List[Relationship] = field(default_factory=list)
    hook_requests: List[IngestionHookRunRequest] = field(default_factory=list)

    @property
    def source_node_is_valid(self) -> bool:
        return self.source.is_valid

    def ingest_source_node(self, strategy: "IngestionStrategy"):
        strategy.ingest_source_node(self.source)

    def ingest_relationships(self, strategy: "IngestionStrategy"):
        for relationship in self.relationships:
            strategy.ingest_relationship(relationship)

    def run_ingest_hooks(self, strategy: "IngestionStrategy"):
        for hook_req in self.hook_requests:
            strategy.run_hook(hook_req.hook, hook_req.before_ingest)

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

    def ingest(self, strategy: "IngestionStrategy"):
        if self.can_perform_ingest():
            self.ingest_source_node(strategy)
            self.ingest_relationships(strategy)
        self.run_ingest_hooks(strategy)

    def add_relationship(
        self,
        related_node: Node,
        details: Relationship,
        outbound: bool,
        match_strategy: MatchStrategy,
    ):
        from_node, to_node = (
            (self.source, related_node) if outbound else (related_node, self.source)
        )
        self.relationships.append(
            RelationshipWithNodes(
                from_node=from_node,
                to_node=to_node,
                details=details,
                match_strategy=match_strategy,
            )
        )

    def add_ingest_hook(self, hook: IngestionHook, before_ingest=False):
        self.hook_requests.append(IngestionHookRunRequest(hook, before_ingest))
