from typing import AsyncGenerator, Iterable

from ..model import IngestionHook, Node, RelationshipWithNodes, TimeToLiveConfiguration
from ..pipeline.pipeline import empty_async_generator
from ..schema.migrations import Migrator
from ..schema.migrations.operations import Operation
from .copy import TypeRetriever
from .database_connector import DatabaseConnector
from .query_executor import (
    OperationOnNodeIdentity,
    OperationOnRelationshipIdentity,
    QueryExecutor,
)


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


class NullRetriver(TypeRetriever):
    def get_nodes_of_type(self, _: str) -> AsyncGenerator[Node, None]:
        return empty_async_generator()

    def get_relationships_of_type(
        self, _: str
    ) -> AsyncGenerator[RelationshipWithNodes, None]:
        return empty_async_generator()


class NullConnector(DatabaseConnector, alias="null"):
    def make_migrator(self) -> TypeRetriever:
        return NullMigrator()

    def make_query_executor(self) -> QueryExecutor:
        return NullQueryExecutor()

    def make_type_retriever(self) -> TypeRetriever:
        return NullRetriver()
