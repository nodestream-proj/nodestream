from typing import AsyncGenerator

from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Relationship as Neo4jRelationship
from nodestream.databases import TypeRetriever
from nodestream.model import Node, PropertySet, Relationship, RelationshipWithNodes

from .extractor import Neo4jExtractor
from .neo4j_database import Neo4jDatabaseConnection

FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT = """
MATCH (n:{type})
RETURN n SKIP $offset LIMIT $limit
"""

FETCH_ALL_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT = """
MATCH (a)-[r:{type}]->(b)
RETURN a, r, b SKIP $offset LIMIT $limit
"""


class Neo4jTypeRetriever(TypeRetriever):
    def __init__(self, database_connection: Neo4jDatabaseConnection) -> None:
        self.database_connection = database_connection

    def map_neo4j_node_to_nodestream_node(
        self, node: Neo4jNode, type: str = None
    ) -> Node:
        # NOTE: I don't think this will work in all cases.
        # But I think this will require shaking out in the future.
        type = type or next(iter(node.labels))
        return Node(
            type=type,
            properties=PropertySet(node),
            additional_types=tuple(label for label in node.labels if label != type),
        )

    def map_neo4j_relationship_to_nodestream_relationship(
        self, relationship: Neo4jRelationship
    ) -> Relationship:
        return Relationship(
            type=relationship.type,
            properties=PropertySet(relationship),
        )

    def get_node_type_extractor(self, type: str) -> Neo4jExtractor:
        return Neo4jExtractor(
            FETCH_ALL_NODES_BY_TYPE_QUERY_FORMAT.format(type=type),
            self.database_connection,
        )

    def get_relationship_type_extractor(self, type: str) -> Neo4jExtractor:
        return Neo4jExtractor(
            FETCH_ALL_RELATIONSHIPS_BY_TYPE_QUERY_FORMAT.format(type=type),
            self.database_connection,
        )

    async def get_nodes_of_type(self, type: str) -> AsyncGenerator[Node, None]:
        extractor = self.get_node_type_extractor(type)
        async for row in extractor.extract_records():
            yield self.map_neo4j_node_to_nodestream_node(row["n"], type=type)

    async def get_relationships_of_type(
        self, type: str
    ) -> AsyncGenerator[RelationshipWithNodes, None]:
        extractor = self.get_relationship_type_extractor(type)
        async for row in extractor.extract_records():
            yield RelationshipWithNodes(
                from_node=self.map_neo4j_node_to_nodestream_node(row["a"]),
                to_node=self.map_neo4j_node_to_nodestream_node(row["b"]),
                relationship=self.map_neo4j_relationship_to_nodestream_relationship(
                    row["r"]
                ),
            )
