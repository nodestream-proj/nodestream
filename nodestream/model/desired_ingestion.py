import asyncio
from dataclasses import asdict, dataclass, field
from logging import getLogger
from typing import TYPE_CHECKING, List

from .graph_objects import Node, Relationship, RelationshipWithNodes
from .ingestion_hooks import IngestionHook, IngestionHookRunRequest
from .match_strategy import MatchStrategy

if TYPE_CHECKING:
    from ..databases.ingest_strategy import IngestionStrategy


LOGGER = getLogger(__name__)


@dataclass(slots=True)
class DesiredIngestion:
    source: Node = field(default_factory=Node)
    relationships: List[Relationship] = field(default_factory=list)
    hook_requests: List[IngestionHookRunRequest] = field(default_factory=list)

    @property
    def source_node_is_valid(self) -> bool:
        return self.source.is_valid

    async def ingest_source_node(self, strategy: "IngestionStrategy"):
        await strategy.ingest_source_node(self.source)

    async def ingest_relationships(self, strategy: "IngestionStrategy"):
        await asyncio.gather(
            *(
                strategy.ingest_relationship(relationship)
                for relationship in self.relationships
            )
        )

    async def run_ingest_hooks(self, strategy: "IngestionStrategy"):
        await asyncio.gather(
            *(
                strategy.run_hook(hook_req.hook, hook_req.before_ingest)
                for hook_req in self.hook_requests
            )
        )

    def can_perform_ingest(self):
        # We can do the main part of the ingest if the source node is valid.
        # If its not valid, its only an error when there are relationships we are
        # trying to ingest as well.
        if not self.source_node_is_valid:
            if len(self.relationships) > 0:
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

    async def ingest(self, strategy: "IngestionStrategy"):
        if self.can_perform_ingest():
            await self.ingest_source_node(strategy)
            await self.ingest_relationships(strategy)
        await self.run_ingest_hooks(strategy)

    def add_relationship(
        self,
        related_node: Node,
        relationship: Relationship,
        outbound: bool,
        match_strategy: MatchStrategy,
    ):
        from_node, to_node = (
            (self.source, related_node) if outbound else (related_node, self.source)
        )
        from_match, to_match = (
            (MatchStrategy.EAGER, match_strategy)
            if outbound
            else (match_strategy, MatchStrategy.EAGER)
        )
        self.relationships.append(
            RelationshipWithNodes(
                from_node=from_node,
                to_node=to_node,
                relationship=relationship,
                from_side_match_strategy=from_match,
                to_side_match_strategy=to_match,
            )
        )

    def add_ingest_hook(self, hook: IngestionHook, before_ingest=False):
        self.hook_requests.append(IngestionHookRunRequest(hook, before_ingest))
