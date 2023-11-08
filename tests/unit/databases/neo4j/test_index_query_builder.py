import pytest
from hamcrest import assert_that, equal_to

from nodestream.databases.neo4j.index_query_builder import (
    Neo4jEnterpriseIndexQueryBuilder,
    Neo4jIndexQueryBuilder,
)
from nodestream.databases.neo4j.query import Query
from nodestream.schema.indexes import FieldIndex, KeyIndex
from nodestream.schema.schema import GraphObjectType


@pytest.fixture
def regular_query_builder() -> Neo4jIndexQueryBuilder:
    return Neo4jIndexQueryBuilder()


@pytest.fixture
def enterprise_query_builder() -> Neo4jEnterpriseIndexQueryBuilder:
    return Neo4jEnterpriseIndexQueryBuilder()


def test_node_field_index_produces_correct_query(regular_query_builder):
    index = FieldIndex("TestNodeType", "foo", GraphObjectType.NODE)
    generated_query = regular_query_builder.create_field_index_query(index, True)
    expected_query = Query.from_statement(
        "CREATE INDEX TestNodeType_foo_additional_index IF NOT EXISTS FOR (n:`TestNodeType`) ON (n.`foo`)",
        True
    )
    assert_that(generated_query, equal_to(expected_query))


def test_rel_field_index_produces_correct_query(regular_query_builder):
    index = FieldIndex("IS_RELATED_TO", "foo", GraphObjectType.RELATIONSHIP)
    generated_query = regular_query_builder.create_field_index_query(index, True)
    expected_query = Query.from_statement(
        "CREATE INDEX IS_RELATED_TO_foo_additional_index IF NOT EXISTS FOR ()-[r:`IS_RELATED_TO`]-() ON (r.`foo`)",
        True
    )
    assert_that(generated_query, equal_to(expected_query))


def test_key_index_single_property_produces_correct_query(regular_query_builder):
    index = KeyIndex("TestNodeType", frozenset(["foo"]))
    generated_query = regular_query_builder.create_key_index_query(index, True)
    expected_query = Query.from_statement(
        "CREATE CONSTRAINT TestNodeType_node_key IF NOT EXISTS FOR (n:`TestNodeType`) REQUIRE (n.`foo`) IS UNIQUE",
        True
    )
    assert_that(generated_query, equal_to(expected_query))


def test_key_multiple_single_property_produces_correct_query(regular_query_builder):
    index = KeyIndex("TestNodeType", frozenset(["foo", "bar"]))
    generated_query = regular_query_builder.create_key_index_query(index, True)
    expected_query = Query.from_statement(
        "CREATE CONSTRAINT TestNodeType_node_key IF NOT EXISTS FOR (n:`TestNodeType`) REQUIRE (n.`bar`,n.`foo`) IS UNIQUE",
        True
    )
    assert_that(generated_query, equal_to(expected_query))


def test_key_index_single_property_produces_correct_query_enterprise(
    enterprise_query_builder,
):
    index = KeyIndex("TestNodeType", frozenset(["foo"]))
    generated_query = enterprise_query_builder.create_key_index_query(index, True)
    expected_query = Query.from_statement(
        "CREATE CONSTRAINT TestNodeType_node_key IF NOT EXISTS FOR (n:`TestNodeType`) REQUIRE (n.`foo`) IS NODE KEY",
        True
    )
    assert_that(generated_query, equal_to(expected_query))


def test_key_multiple_single_property_produces_correct_query_enterprise(
    enterprise_query_builder,
):
    index = KeyIndex("TestNodeType", frozenset(["foo", "bar"]))
    generated_query = enterprise_query_builder.create_key_index_query(index, True)
    expected_query = Query.from_statement(
        "CREATE CONSTRAINT TestNodeType_node_key IF NOT EXISTS FOR (n:`TestNodeType`) REQUIRE (n.`bar`,n.`foo`) IS NODE KEY",
        True
    )
    assert_that(generated_query, equal_to(expected_query))
