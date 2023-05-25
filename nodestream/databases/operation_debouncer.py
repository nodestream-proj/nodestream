from collections import defaultdict
from typing import Dict, Iterable, List, Tuple

from ..model import MatchStrategy, Node, RelationshipWithNodes
from .query_executor import OperationOnNodeIdentity, OperationOnRelationshipIdentity


class OperationDebouncer:
    def __init__(self):
        self.nodes_by_shape_operation: Dict[
            OperationOnNodeIdentity, List[Node]
        ] = defaultdict(list)
        self.relationships_by_operation: Dict[
            OperationOnRelationshipIdentity, List[RelationshipWithNodes]
        ] = defaultdict(list)

    def bucketize_node_operation(
        self, node: Node, match_strategy: MatchStrategy
    ) -> List[Node]:
        return self.nodes_by_shape_operation[
            OperationOnNodeIdentity(node.identity_shape, match_strategy)
        ]

    def bucketize_relationship_operation(
        self, relationship: RelationshipWithNodes
    ) -> List[RelationshipWithNodes]:
        return self.relationships_by_operation[
            OperationOnRelationshipIdentity(
                from_node=OperationOnNodeIdentity(
                    node_identity=relationship.from_node.identity_shape,
                    match_strategy=relationship.from_side_match_strategy.prevent_creation(),
                ),
                to_node=OperationOnNodeIdentity(
                    node_identity=relationship.to_node.identity_shape,
                    match_strategy=relationship.to_side_match_strategy.prevent_creation(),
                ),
                relationship_identity=relationship.relationship.identity_shape,
            )
        ]

    def debounce_node_operation(
        self, node: Node, match_strategy: MatchStrategy = MatchStrategy.EAGER
    ):
        bucket = self.bucketize_node_operation(node, match_strategy)
        for existing_node in bucket:
            if existing_node.has_same_key(node):
                existing_node.update(node)
                break
        else:
            bucket.append(node)

    def debounce_relationship(self, relationship: RelationshipWithNodes):
        bucket = self.bucketize_relationship_operation(relationship)
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
    ) -> Iterable[Tuple[OperationOnRelationshipIdentity, List[RelationshipWithNodes]]]:
        yield from self.relationships_by_operation.items()
        self.relationships_by_operation.clear()
