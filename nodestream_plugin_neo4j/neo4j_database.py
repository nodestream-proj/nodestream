from logging import getLogger
from typing import Iterable

from neo4j import AsyncDriver, AsyncGraphDatabase, AsyncSession, Record, RoutingControl

from .query import Query


class Neo4jDatabaseConnection:
    @classmethod
    def from_configuration(
        cls,
        uri: str,
        username: str,
        password: str,
        database_name: str = "neo4j",
        **driver_kwargs
    ):
        driver = AsyncGraphDatabase.driver(
            uri, auth=(username, password), **driver_kwargs
        )
        return cls(driver, database_name)

    def __init__(self, driver: AsyncDriver, database_name: str) -> None:
        self.driver = driver
        self.database_name = database_name
        self.logger = getLogger(self.__class__.__name__)

    async def execute(
        self, query: Query, log_result: bool = False, routing=RoutingControl.WRITE
    ) -> Iterable[Record]:
        self.logger.info(
            "Executing Cypher Query to Neo4j",
            extra={
                "query": query.query_statement,
                "uri": self.driver._pool.address.host,
            },
        )

        result = await self.driver.execute_query(
            query.query_statement,
            query.parameters,
            database_=self.database_name,
            routing_=routing,
        )
        for record in result.records:
            if log_result:
                self.logger.info(
                    "Gathered Query Results",
                    extra=dict(**record, query=query.query_statement),
                )

        return result.records

    def session(self) -> AsyncSession:
        return self.driver.session(database=self.database_name)
