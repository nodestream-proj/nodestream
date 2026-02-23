from typing import Any, AsyncGenerator, Coroutine, Iterable

from ..model import IngestionHook, Node, RelationshipWithNodes, TimeToLiveConfiguration
from ..schema.migrations import Migrator
from ..schema.migrations.operations import Operation
from .copy import TypeRetriever
from .database_connector import DatabaseConnector
from .query_executor import (
    OperationOnNodeIdentity,
    OperationOnRelationshipIdentity,
    QueryExecutor,
)


async def empty_async_generator():
    for i in []:
        yield i  # pragma: no cover


class NullMigrator(Migrator):
    async def execute_operation(self, _: Operation):
        pass


class NullQueryExecutor(QueryExecutor):
    async def upsert_nodes_in_bulk_with_same_operation(
        self, _: OperationOnNodeIdentity, __: Iterable[Node]
    ):
        pass

    async def upsert_relationships_in_bulk_of_same_operation(
        self,
        _: OperationOnRelationshipIdentity,
        __: Iterable[RelationshipWithNodes],
    ):
        pass

    async def perform_ttl_op(self, _: TimeToLiveConfiguration):
        pass

    async def execute_hook(self, _: IngestionHook):
        pass

    async def finish(self):
        """Close connector by calling finish method from Step"""
        pass


class NullRetriver(TypeRetriever):
    async def preview_node_count(self, _: str) -> Coroutine[int, Any, Any]:
        return 0

    async def preview_relationship_count(self, _: str) -> Coroutine[int, Any, Any]:
        return 0

    def get_nodes_of_type(self, _: str) -> AsyncGenerator[Node, None]:
        return empty_async_generator()

    def get_relationships_of_type_between(
        self, __: str, ___: str, ____: str
    ) -> AsyncGenerator[RelationshipWithNodes, None]:
        return empty_async_generator()

    def get_relationships_of_type(
        self, _: str
    ) -> AsyncGenerator[RelationshipWithNodes, None]:
        return empty_async_generator()


class NullConnector(DatabaseConnector, alias="null"):
    def __init__(self, **_) -> None:
        pass

    def make_migrator(self) -> TypeRetriever:
        return NullMigrator()

    def make_query_executor(self) -> QueryExecutor:
        return NullQueryExecutor()

    def make_type_retriever(self, **kwargs) -> TypeRetriever:
        return NullRetriver()
