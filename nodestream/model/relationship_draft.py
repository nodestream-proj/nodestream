from dataclasses import dataclass
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
        if self.outbound:
            from_node, from_match = (source_node, source_creation_rule)
            to_node, to_match = (self.related_node, self.related_node_creation_rule)
        else:
            from_node, from_match = (self.related_node, self.related_node_creation_rule)
            to_node, to_match = (source_node, source_creation_rule)

        return RelationshipWithNodes(
            from_node=from_node,
            to_node=to_node,
            relationship=self.relationship,
            from_side_node_creation_rule=from_match,
            to_side_node_creation_rule=to_match,
            relationship_creation_rule=self.relationship_creation_rule,
        )
