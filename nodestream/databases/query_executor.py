from abc import ABC, abstractmethod
from typing import Iterable

from ..model import (
    RelationshipWithNodesIdentityShape,
    NodeIdentityShape,
    Node,
    RelationshipWithNodes,
    KeyIndex,
    FieldIndex,
    TimeToLiveConfiguration,
    IngestionHook,
)
from ..subclass_registry import SubclassRegistry

QUERY_EXECUTOR_SUBCLASS_REGISTRY = SubclassRegistry()


@QUERY_EXECUTOR_SUBCLASS_REGISTRY.connect_baseclass
class QueryExecutor(ABC):
    @classmethod
    def from_database_args(cls, database: str = "neo4j", **database_args):
        return QUERY_EXECUTOR_SUBCLASS_REGISTRY.get(database).from_file_arguments(
            **database_args
        )

    @classmethod
    @abstractmethod
    def from_file_arguments(cls, **kwargs):
        raise NotImplementedError

    @abstractmethod
    async def upsert_nodes_in_bulk_of_same_shape(
        self, shape: NodeIdentityShape, nodes: Iterable[Node]
    ):
        raise NotImplementedError

    @abstractmethod
    async def upsert_relationships_in_bulk_of_same_shape(
        self,
        shape: RelationshipWithNodesIdentityShape,
        rels: Iterable[RelationshipWithNodes],
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
