from typing import Iterable

from ..model import IngestionHook, Node, RelationshipWithNodes, TimeToLiveConfiguration
from ..schema.migrations import Migrator
from ..schema.migrations.operations import Operation
from .copy import TypeHistogram, TypeRetriever
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


class NullRetriever(TypeRetriever):
    def __init__(self, **_):
        from ..schema.state import Schema

        super().__init__(schema=Schema())

    async def build_histogram(self) -> TypeHistogram:
        return TypeHistogram()

    async def fetch_extractors(self):
        return
        yield  # pragma: no cover


class NullConnector(DatabaseConnector, alias="null"):
    def __init__(self, **_) -> None:
        pass

    def make_migrator(self) -> Migrator:
        return NullMigrator()

    def make_query_executor(self) -> QueryExecutor:
        return NullQueryExecutor()

    def make_type_retriever(self, **kwargs) -> TypeRetriever:
        return NullRetriever()
