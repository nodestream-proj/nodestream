from datetime import datetime

import pytest
from hamcrest import assert_that, equal_to
from freezegun import freeze_time

from nodestream.model import (
    TimeToLiveConfiguration,
    GraphObjectType,
    Node,
    MatchStrategy,
)
from nodestream.databases.neo4j import Neo4jIngestQueryBuilder
from nodestream.databases.query_executor import OperationOnNodeIdentity
from nodestream.databases.neo4j.query import Query, QueryBatch


@pytest.fixture
def query_builder():
    return Neo4jIngestQueryBuilder()


GREATEST_DAY = datetime(1998, 3, 25, 2, 0, 1)

BASIC_NODE_TTL = TimeToLiveConfiguration(
    graph_type=GraphObjectType.NODE, object_type="TestNodeType", expiry_in_hours=10
)
BASIC_NODE_TTL_EXPECTED_QUERY = Query(
    "MATCH (x: TestNodeType) WHERE x.last_ingested_at <= $earliest_allowed_time RETURN id(x) as id",
    {"earliest_allowed_time": GREATEST_DAY},
)

NODE_TTL_WITH_CUSTOM_QUERY = TimeToLiveConfiguration(
    graph_type=GraphObjectType.NODE,
    object_type="TestNodeType",
    custom_query="MATCH (n:TestNodeType) RETURN n",
    expiry_in_hours=10,
)
NODE_TTL_WITH_CUSTOM_QUERY_EXPECTED_QUERY = Query(
    "MATCH (n:TestNodeType) RETURN n", {"earliest_allowed_time": GREATEST_DAY}
)

BASIC_REL_TTL = TimeToLiveConfiguration(
    graph_type=GraphObjectType.RELATIONSHIP,
    object_type="IS_RELATED_TO",
    expiry_in_hours=10,
)
BASIC_REL_TTL_EXPECTED_QUERY = Query(
    "MATCH ()-[x: IS_RELATED_TO]->() WHERE x.last_ingested_at <= $earliest_allowed_time RETURN id(x) as id",
    {"earliest_allowed_time": GREATEST_DAY},
)

REL_TTL_WITH_CUSTOM_QUERY = TimeToLiveConfiguration(
    graph_type=GraphObjectType.RELATIONSHIP,
    object_type="IS_RELATED_TO",
    custom_query="MATCH ()-[x: IS_RELATED_TO]->() RETURN x",
    expiry_in_hours=10,
)
REL_TTL_WITH_CUSTOM_QUERY_EXPECTED_QUERY = Query(
    "MATCH ()-[x: IS_RELATED_TO]->() RETURN x",
    {"earliest_allowed_time": GREATEST_DAY},
)


@pytest.mark.parametrize(
    "ttl,expected_query",
    [
        (BASIC_NODE_TTL, BASIC_NODE_TTL_EXPECTED_QUERY),
        (NODE_TTL_WITH_CUSTOM_QUERY, NODE_TTL_WITH_CUSTOM_QUERY_EXPECTED_QUERY),
        (BASIC_REL_TTL, BASIC_REL_TTL_EXPECTED_QUERY),
        (REL_TTL_WITH_CUSTOM_QUERY, REL_TTL_WITH_CUSTOM_QUERY_EXPECTED_QUERY),
    ],
)
@freeze_time("1998-03-25 12:00:01")
def test_generates_expected_queries(query_builder, ttl, expected_query):
    resultant_query = query_builder.generate_ttl_query_from_configuration(ttl)
    assert_that(resultant_query, equal_to(expected_query))


# In a simple node case, we should MERGE the node on the basis of its identity shape
# and then set all of the properties on the node.
SIMPLE_NODE = Node("TestType", {"id": "foo"})
SIMPLE_NODE_EXPECTED_QUERY = QueryBatch(
    "MERGE (node: TestType {id : params.__node_id}) SET node += params.__node_properties",
    [
        {
            "__node_id": "foo",
            "__node_properties": SIMPLE_NODE.properties,
            "__node_additional_labels": (),
        }
    ],
)

# In a more complex node case, we should still MERGE the node on the basis of its identity shape
# but, in addition, we should add any additional labels that the node has.
COMPLEX_NODE = Node(
    "ComplexType",
    {"id": "foo"},
    additional_types=("ExtraTypeOne", "ExtraTypeTwo"),
)
COMPLEX_NODE_EXPECTED_QUERY = QueryBatch(
    "MERGE (node: ComplexType {id : params.__node_id}) SET node += params.__node_properties WITH node, params CALL apoc.create.addLabels(node, params.__node_additional_labels) yield node RETURN true",
    [
        {
            "__node_id": "foo",
            "__node_properties": COMPLEX_NODE.properties,
            "__node_additional_labels": ("ExtraTypeOne", "ExtraTypeTwo"),
        }
    ],
)


@pytest.mark.parametrize(
    "node,expected_query",
    [
        [SIMPLE_NODE, SIMPLE_NODE_EXPECTED_QUERY],
        [COMPLEX_NODE, COMPLEX_NODE_EXPECTED_QUERY],
    ],
)
def test_node_update_generates_expected_queries(query_builder, node, expected_query):
    operation = OperationOnNodeIdentity(node.identity_shape, MatchStrategy.EAGER)
    query = query_builder.generate_batch_update_node_operation_batch(operation, [node])
    assert_that(query, equal_to(expected_query))


# def test_generate_ingest_source_query_single_key():
#     result_query, result_params = generate_ingest_source_query(node)
#     expected_query = "WITH params MERGE (source: TestType {id : params.__source_id}) SET source += params.__source_properties"

#     assert result_query == expected_query
#     assert result_params["__source_id"] == "foo"
#     assert result_params["__source_properties"] == node.metadata


# def test_generate_ingest_source_query_multi_label():
#     node =
#     result_query, result_params = generate_ingest_source_query(node)
#     expected_query = "WITH params MERGE (source: TestType {id : params.__source_id}) SET source += params.__source_properties WITH source, params CALL apoc.create.addLabels(source, params.__source_additional_labels) yield node RETURN true"

#     assert result_query == expected_query
#     assert result_params["__source_id"] == "foo"
#     assert result_params["__source_properties"] == node.metadata
#     assert result_params["__source_additional_labels"] == node.additional_types


# def test_generate_ingest_source_complex_case():
#     node = SourceNode("TestType", {"id": "foo"})
#     node.metadata.add_metadata("meta", "value")
#     result_query, result_params = generate_ingest_source_query(node)
#     expected_query = "WITH params MERGE (source: TestType {id : params.__source_id}) SET source += params.__source_properties"

#     assert result_query == expected_query
#     assert result_params["__source_id"] == "foo"
#     assert result_params["__source_properties"] == node.metadata
