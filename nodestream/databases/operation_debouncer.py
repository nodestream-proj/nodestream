from collections import defaultdict
from typing import Dict, Iterable, Tuple

from ..model.graph_objects import (
    DeduplicatableObject,
    MatchStrategy,
    Node,
    RelationshipWithNodes,
)
from .query_executor import OperationOnNodeIdentity, OperationOnRelationshipIdentity


class DeduplicationBucket:
    __slots__ = ("bucket",)

    def __init__(self) -> None:
        self.bucket: Dict[tuple, DeduplicatableObject] = {}

    def include(self, deduplicatable: DeduplicatableObject):
        key = deduplicatable.get_dedup_key()
        if existing_operation := self.bucket.get(key):
            existing_operation.update(deduplicatable)
        else:
            self.bucket[key] = deduplicatable

    def values(self) -> Iterable[DeduplicatableObject]:
        return self.bucket.values()


class OperationBucketGroup:
    __slots__ = ("buckets",)

    def __init__(self) -> None:
        self.buckets = defaultdict(DeduplicationBucket)

    def get_bucket(self, key: Tuple) -> DeduplicationBucket:
        return self.buckets[key]

    def drain(self) -> Iterable[Tuple[Tuple, Iterable[DeduplicatableObject]]]:
        for key, bucket in self.buckets.items():
            yield key, bucket.values()

        self.buckets.clear()


class OperationDebouncer:
    __slots__ = ("node_operation_buckets", "relationship_operation_buckets")

    def __init__(self):
        self.node_operation_buckets = OperationBucketGroup()
        self.relationship_operation_buckets = OperationBucketGroup()

    def bucketize_node_operation(
        self, node: Node, match_strategy: MatchStrategy
    ) -> DeduplicationBucket:
        return self.node_operation_buckets.get_bucket(
            OperationOnNodeIdentity(node.identity_shape, match_strategy)
        )

    def bucketize_relationship_operation(
        self, relationship: RelationshipWithNodes
    ) -> DeduplicationBucket:
        return self.relationship_operation_buckets.get_bucket(
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
        )

    def debounce_node_operation(
        self, node: Node, match_strategy: MatchStrategy = MatchStrategy.EAGER
    ):
        bucket = self.bucketize_node_operation(node, match_strategy)
        bucket.include(node)

    def debounce_relationship(self, relationship: RelationshipWithNodes):
        bucket = self.bucketize_relationship_operation(relationship)
        bucket.include(relationship)

    def drain_node_groups(
        self,
    ) -> Iterable[Tuple[OperationOnNodeIdentity, Iterable[Node]]]:
        yield from self.node_operation_buckets.drain()

    def drain_relationship_groups(
        self,
    ) -> Iterable[
        Tuple[OperationOnRelationshipIdentity, Iterable[RelationshipWithNodes]]
    ]:
        yield from self.relationship_operation_buckets.drain()
