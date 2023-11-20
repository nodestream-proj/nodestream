from logging import getLogger
from typing import Any, Dict, Optional

from neo4j import RoutingControl

from ...pipeline.extractors import Extractor
from .database_connector import Neo4jDatabaseConnector


class Neo4jExtractor(Extractor):
    @classmethod
    def from_file_data(
        cls,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        **connector_args
    ):
        connector = Neo4jDatabaseConnector.from_file_data(**connector_args)
        return cls(query, connector, parameters, limit)

    def __init__(
        self,
        query: str,
        connector: Neo4jDatabaseConnector,
        parameters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
    ) -> None:
        self.connector = connector
        self.query = query
        self.parameters = parameters or {}
        self.limit = limit
        self.logger = getLogger(self.__class__.__name__)

    async def extract_records(self):
        # TODO: In the future, we should extract the database pagination logic from
        # this class and move it to a GraphDatabaseExtractor class following the lead
        # we have of the writer class.
        offset = 0
        should_continue = True
        driver = self.connector.driver
        database_name = self.connector.database_name

        while should_continue:
            params = dict(**self.parameters, limit=self.limit, offset=offset)
            self.logger.info(
                "Running query on neo4j",
                extra=dict(query=self.query, params=params),
            )
            query_results, _, _ = await driver.execute_query(
                self.query,
                params,
                routing_=RoutingControl.READ,
                database_=database_name,
            )
            returned_records = list(query_results)
            should_continue = len(returned_records) > 0
            offset += self.limit
            for item in returned_records:
                yield item
