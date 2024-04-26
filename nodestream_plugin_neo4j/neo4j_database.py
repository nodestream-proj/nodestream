from logging import getLogger
from typing import Awaitable, Iterable, Tuple, Union

from neo4j import AsyncDriver, AsyncGraphDatabase, AsyncSession, Record, RoutingControl
from neo4j.auth_management import AsyncAuthManagers
from nodestream.file_io import LazyLoadedArgument

from .query import Query


def auth_provider_factory(
    username: Union[str, LazyLoadedArgument],
    password: Union[str, LazyLoadedArgument],
) -> Awaitable[Tuple[str, str]]:
    logger = getLogger(__name__)

    async def auth_provider():
        logger.info("Fetching new neo4j credentials")

        if isinstance(username, LazyLoadedArgument):
            logger.debug("Fetching username since value is lazy loaded")
            current_username = username.get_value()
        else:
            current_username = username

        if isinstance(password, LazyLoadedArgument):
            logger.debug("Fetching password since value is lazy loaded")
            current_password = password.get_value()
        else:
            current_password = password

        return current_username, current_password

    return auth_provider


class Neo4jDatabaseConnection:
    @classmethod
    def from_configuration(
        cls,
        uri: str,
        username: Union[str, LazyLoadedArgument],
        password: Union[str, LazyLoadedArgument],
        database_name: str = "neo4j",
        **driver_kwargs
    ):
        auth = AsyncAuthManagers.basic(auth_provider_factory(username, password))
        driver = AsyncGraphDatabase.driver(uri, auth=auth, **driver_kwargs)
        return cls(driver, database_name)

    def __init__(self, driver: AsyncDriver, database_name: str) -> None:
        self.driver = driver
        self.database_name = database_name
        self.logger = getLogger(self.__class__.__name__)

    async def execute(
        self, query: Query, log_result: bool = False, routing_=RoutingControl.WRITE
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
            routing_=routing_,
        )
        if log_result:
            for record in result.records:
                self.logger.info(
                    "Gathered Query Results",
                    extra=dict(
                        **record,
                        query=query.query_statement,
                        uri=self.driver._pool.address.host
                    ),
                )
        return result.records

    def session(self) -> AsyncSession:
        return self.driver.session(database=self.database_name)
