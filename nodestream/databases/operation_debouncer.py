from collections import defaultdict
from typing import Dict, List, Iterable, Tuple

from ..model import (
    Node,
    RelationshipWithNodes,
    RelationshipWithNodesIdentityShape,
    MatchStrategy,
)

from .query_executor import OperationOnNodeIdentity


class OperationDebouncer:
    def __init__(self):
        self.nodes_by_shape_operation: Dict[
            OperationOnNodeIdentity, List[Node]
        ] = defaultdict(list)
        self.relationships_by_shape: Dict[
            RelationshipWithNodesIdentityShape, List[RelationshipWithNodes]
        ] = defaultdict(list)

    def debounce_node_operation(
        self, node: Node, match_strategy: MatchStrategy = MatchStrategy.EAGER
    ):
        bucket = self.nodes_by_shape_operation[
            OperationOnNodeIdentity(node.identity_shape, match_strategy)
        ]
        for existing_node in bucket:
            if existing_node.has_same_key(node):
                existing_node.update(node)
                break
        else:
            bucket.append(node)

    def debounce_relationship(self, relationship: RelationshipWithNodes):
        bucket = self.relationships_by_shape[relationship.identity_shape]
        for existing_rel in bucket:
            if existing_rel.has_same_keys(relationship):
                existing_rel.update(relationship)
                break
        else:
            bucket.append(relationship)

    def drain_node_groups(
        self,
    ) -> Iterable[Tuple[OperationOnNodeIdentity, List[Node]]]:
        yield from self.nodes_by_shape_operation.items()
        self.nodes_by_shape_operation.clear()

    def drain_relationship_groups(
        self,
    ) -> Iterable[
        Tuple[RelationshipWithNodesIdentityShape, List[RelationshipWithNodes]]
    ]:
        yield from self.relationships_by_shape.items()
        self.relationships_by_shape.clear()
