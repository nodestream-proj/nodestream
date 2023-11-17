from abc import ABC, abstractmethod

from ..pluggable import Pluggable
from ..subclass_registry import SubclassRegistry
from .copy import TypeRetriever
from .query_executor import QueryExecutor
from .query_executor_with_statistics import QueryExecutorWithStatistics

DATABASE_CONNECTOR_SUBCLASS_REGISTRY = SubclassRegistry()


@DATABASE_CONNECTOR_SUBCLASS_REGISTRY.connect_baseclass
class DatabaseConnector(ABC, Pluggable):
    entrypoint_name = "databases"

    @classmethod
    def from_database_args(
        cls, database: str = "neo4j", **database_args
    ) -> "DatabaseConnector":
        DatabaseConnector.import_all()

        return DATABASE_CONNECTOR_SUBCLASS_REGISTRY.get(database).from_file_data(
            **database_args
        )

    @classmethod
    def from_file_data(cls, **kwargs):
        return cls(**kwargs)

    @abstractmethod
    def make_query_executor(self) -> QueryExecutor:
        raise NotImplementedError

    @abstractmethod
    def make_type_retriever(self) -> TypeRetriever:
        raise NotImplementedError

    def get_query_executor(self, collect_stats: bool = True):
        connector = self.make_query_executor()
        if collect_stats:
            connector = QueryExecutorWithStatistics(connector)
        return connector

    def get_type_retriever(self):
        return self.make_type_retriever()
