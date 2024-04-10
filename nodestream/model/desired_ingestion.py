import asyncio
from dataclasses import asdict, dataclass, field
from logging import getLogger
from typing import TYPE_CHECKING, List

from .creation_rules import NodeCreationRule, RelationshipCreationRule
from .graph_objects import Node, Relationship
from .ingestion_hooks import IngestionHook, IngestionHookRunRequest
from .relationship_draft import RelationshipDraft

if TYPE_CHECKING:
    from ..databases.ingest_strategy import IngestionStrategy


LOGGER = getLogger(__name__)


@dataclass(slots=True)
class DesiredIngestion:
    source: Node = field(default_factory=Node)
    relationships: List[RelationshipDraft] = field(default_factory=list)
    hook_requests: List[IngestionHookRunRequest] = field(default_factory=list)
    creation_rule: NodeCreationRule = None

    @property
    def source_node_is_valid(self) -> bool:
        return self.source.is_valid and self.creation_rule is not None

    async def ingest_source_node(self, strategy: "IngestionStrategy"):
        await strategy.ingest_source_node(self.source, self.creation_rule)

    async def ingest_relationships(self, strategy: "IngestionStrategy"):
        await asyncio.gather(
            *(
                strategy.ingest_relationship(
                    relationship.make_relationship(
                        source_node=self.source, source_creation_rule=self.creation_rule
                    )
                )
                for relationship in self.relationships
            )
        )

    async def run_ingest_hooks(self, strategy: "IngestionStrategy"):
        await asyncio.gather(
            *(strategy.run_hook(hook_req) for hook_req in self.hook_requests)
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
        node_creation_rule: NodeCreationRule = NodeCreationRule.EAGER,
        relationship_creation_rule: RelationshipCreationRule = RelationshipCreationRule.EAGER,
    ):
        self.relationships.append(
            RelationshipDraft(
                related_node=related_node,
                relationship=relationship,
                outbound=outbound,
                related_node_creation_rule=node_creation_rule,
                relationship_creation_rule=relationship_creation_rule,
            )
        )

    def add_ingest_hook(self, hook: IngestionHook, before_ingest=False):
        self.hook_requests.append(IngestionHookRunRequest(hook, before_ingest))
