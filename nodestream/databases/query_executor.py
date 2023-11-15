from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable

from ..model import (
    IngestionHook,
    Node,
    NodeCreationRule,
    NodeIdentityShape,
    RelationshipCreationRule,
    RelationshipIdentityShape,
    RelationshipWithNodes,
    TimeToLiveConfiguration,
)
from ..schema.indexes import FieldIndex, KeyIndex


@dataclass(slots=True, frozen=True)
class OperationOnNodeIdentity:
    node_identity: NodeIdentityShape
    node_creation_rule: NodeCreationRule


@dataclass(slots=True, frozen=True)
class OperationOnRelationshipIdentity:
    from_node: OperationOnNodeIdentity
    to_node: OperationOnNodeIdentity
    relationship_identity: RelationshipIdentityShape
    relationship_creation_rule: RelationshipCreationRule


class QueryExecutor(ABC):
    @abstractmethod
    async def upsert_nodes_in_bulk_with_same_operation(
        self, operation: OperationOnNodeIdentity, nodes: Iterable[Node]
    ):
        raise NotImplementedError

    @abstractmethod
    async def upsert_relationships_in_bulk_of_same_operation(
        self,
        shape: OperationOnRelationshipIdentity,
        relationships: Iterable[RelationshipWithNodes],
    ):
        raise NotImplementedError

    @abstractmethod
    async def upsert_key_index(self, index: KeyIndex):
        raise NotImplementedError

    @abstractmethod
    async def upsert_field_index(self, index: FieldIndex):
        raise NotImplementedError

    @abstractmethod
    async def perform_ttl_op(self, config: TimeToLiveConfiguration):
        raise NotImplementedError

    @abstractmethod
    async def execute_hook(self, hook: IngestionHook):
        raise NotImplementedError
