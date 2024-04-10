from dataclasses import asdict, dataclass
from logging import getLogger
from typing import Optional

from .creation_rules import NodeCreationRule, RelationshipCreationRule
from .graph_objects import Node, Relationship, RelationshipWithNodes

LOGGER = getLogger(__name__)


@dataclass(slots=True)
class RelationshipDraft:
    """A RelationshipDraft that allows to add relationships from interpretations before
    source node is interpreted
    """

    related_node: Node
    related_node_creation_rule: NodeCreationRule
    relationship: Relationship
    relationship_creation_rule: RelationshipCreationRule
    outbound: bool

    def make_relationship(
        self, source_node: Node, source_creation_rule: NodeCreationRule
    ) -> Optional[RelationshipWithNodes]:
        if not self.related_node.is_valid:
            LOGGER.warning(
                "Identity value for related node was null. Skipping.",
                extra=asdict(self.related_node),
            )
            return

        from_node, to_node = (
            (source_node, self.related_node)
            if self.outbound
            else (self.related_node, source_node)
        )
        from_match, to_match = (
            (source_creation_rule, self.related_node_creation_rule)
            if self.outbound
            else (self.related_node_creation_rule, source_creation_rule)
        )
        return RelationshipWithNodes(
            from_node=from_node,
            to_node=to_node,
            relationship=self.relationship,
            from_side_node_creation_rule=from_match,
            to_side_node_creation_rule=to_match,
            relationship_creation_rule=self.relationship_creation_rule,
        )
