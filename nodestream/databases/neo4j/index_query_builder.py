from ...schema.indexes import FieldIndex, KeyIndex
from ...schema.schema import GraphObjectType
from .query import Query

KEY_INDEX_QUERY_FORMAT = "CREATE CONSTRAINT {constraint_name} IF NOT EXISTS FOR (n:`{type}`) REQUIRE ({key_pattern}) IS UNIQUE"
ENTERPRISE_KEY_INDEX_QUERY_FORMAT = "CREATE CONSTRAINT {constraint_name} IF NOT EXISTS FOR (n:`{type}`) REQUIRE ({key_pattern}) IS NODE KEY"
NODE_FIELD_INDEX_QUERY_FORMAT = (
    "CREATE INDEX {constraint_name} IF NOT EXISTS FOR (n:`{type}`) ON (n.`{field}`)"
)
REL_FIELD_INDEX_QUERY_FORMAT = "CREATE INDEX {constraint_name} IF NOT EXISTS FOR ()-[r:`{type}`]-() ON (r.`{field}`)"


def key_index_from_format(key_index: KeyIndex, format: str) -> Query:
    key_pattern = ",".join(f"n.`{p}`" for p in sorted(key_index.identity_keys))
    constraint_name = f"{key_index.type}_node_key"
    statement = format.format(
        constraint_name=constraint_name,
        key_pattern=key_pattern,
        type=key_index.type,
    )
    return Query.from_statement(statement)


class Neo4jIndexQueryBuilder:
    """Creates index creation queries that will work for any Neo4j Database Supported."""

    def create_key_index_query(self, key_index: KeyIndex) -> Query:
        """Creates a key index using a "Unique node property constraint" constraint which is supported by community.

        see: https://neo4j.com/docs/cypher-manual/current/constraints/
        """
        return key_index_from_format(key_index, KEY_INDEX_QUERY_FORMAT)

    def create_field_index_query(self, field_index: FieldIndex) -> Query:
        """Creates a filed index using a 'Range Index' or equivalent.

        see: https://neo4j.com/docs/cypher-manual/current/indexes-for-search-performance/#indexes-create-indexes
        """
        constraint_name = f"{field_index.type}_{field_index.field}_additional_index"
        format = (
            REL_FIELD_INDEX_QUERY_FORMAT
            if field_index.object_type == GraphObjectType.RELATIONSHIP
            else NODE_FIELD_INDEX_QUERY_FORMAT
        )
        statement = format.format(
            constraint_name=constraint_name,
            type=field_index.type,
            field=field_index.field,
        )
        return Query.from_statement(statement)


class Neo4jEnterpriseIndexQueryBuilder(Neo4jIndexQueryBuilder):
    """Creates index creation query that will work only for Neo4j Enterprise"""

    def create_key_index_query(self, key_index: KeyIndex) -> Query:
        """Generates a key index using a `Node key constraint` which is an enterprise feature.

        see: https://neo4j.com/docs/cypher-manual/current/constraints/
        """
        return key_index_from_format(key_index, ENTERPRISE_KEY_INDEX_QUERY_FORMAT)
