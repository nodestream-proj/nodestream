import asyncio
from dataclasses import asdict, dataclass, field
from logging import getLogger
from typing import TYPE_CHECKING, List, Tuple

from .creation_rules import NodeCreationRule, RelationshipCreationRule
from .graph_objects import Node, PropertySet, Relationship, RelationshipWithNodes
from .ingestion_hooks import IngestionHook, IngestionHookRunRequest

if TYPE_CHECKING:
    from ..databases.ingest_strategy import IngestionStrategy


LOGGER = getLogger(__name__)


@dataclass(slots=True)
class DesiredIngestion:
    source: Node = field(default_factory=Node)
    relationships: List[RelationshipWithNodes] = field(default_factory=list)
    hook_requests: List[IngestionHookRunRequest] = field(default_factory=list)
    source_node_creation_rule: NodeCreationRule = NodeCreationRule.EAGER

    @property
    def source_node_is_valid(self) -> bool:
        return self.source.is_valid and self.source_node_creation_rule is not None

    async def ingest_source_node(self, strategy: "IngestionStrategy"):
        await strategy.ingest_source_node(self.source, self.source_node_creation_rule)

    async def ingest_relationships(self, strategy: "IngestionStrategy"):
        await asyncio.gather(
            *(
                strategy.ingest_relationship(relationship)
                for relationship in self.relationships
            )
        )

    async def run_ingest_hooks(self, strategy: "IngestionStrategy"):
        await asyncio.gather(
            *(strategy.run_hook(hook_req) for hook_req in self.hook_requests)
        )

    def can_perform_ingest(self):
        # We can do the main part of the ingest if the source node is valid.
        # If it's not valid, it's only an error when there are relationships we are
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

    def add_source_node(
        self,
        source_type: str,
        additional_types: Tuple[str],
        creation_rule: NodeCreationRule,
        key_values: PropertySet,
        properties: PropertySet,
    ) -> Node:
        self.source.type = source_type
        self.source.additional_types = additional_types
        self.source_node_creation_rule = creation_rule
        self.source.key_values.merge(key_values)
        self.source.properties.merge(properties)
        # Because relationships can be added before the source node
        self.finalize_relationships()
        return self.source

    def finalize_relationships(self):
        """Finalizes relationships that were added before source node.
        Assumes source node has been added
        """
        for relationship in self.relationships:
            if relationship.outbound:
                relationship.from_node = self.source
                relationship.from_side_node_creation_rule = (
                    self.source_node_creation_rule
                )
            else:
                relationship.to_node = self.source
                relationship.to_side_node_creation_rule = self.source_node_creation_rule

    def add_relationship(
        self,
        related_node: Node,
        relationship: Relationship,
        outbound: bool,
        node_creation_rule: NodeCreationRule = NodeCreationRule.EAGER,
        relationship_creation_rule: RelationshipCreationRule = RelationshipCreationRule.EAGER,
    ):
        if not related_node.is_valid:
            LOGGER.warning(
                "Identity value for related node was null. Skipping.",
                extra=asdict(related_node),
            )
            return

        if outbound:
            from_node, from_match = (self.source, self.source_node_creation_rule)
            to_node, to_match = (related_node, node_creation_rule)
        else:
            from_node, from_match = (related_node, node_creation_rule)
            to_node, to_match = (self.source, self.source_node_creation_rule)

        self.relationships.append(
            RelationshipWithNodes(
                from_node=from_node,
                to_node=to_node,
                outbound=outbound,
                relationship=relationship,
                from_side_node_creation_rule=from_match,
                to_side_node_creation_rule=to_match,
                relationship_creation_rule=relationship_creation_rule,
            )
        )

    def add_ingest_hook(self, hook: IngestionHook, before_ingest=False):
        self.hook_requests.append(IngestionHookRunRequest(hook, before_ingest))
