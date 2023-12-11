from datetime import datetime

import pytest
from freezegun import freeze_time
from hamcrest import assert_that, equal_to, equal_to_ignoring_whitespace

from nodestream.databases.neo4j.ingest_query_builder import (
    DELETE_NODE_QUERY,
    DELETE_REL_QUERY,
    Neo4jIngestQueryBuilder,
)
from nodestream.databases.neo4j.query import COMMIT_QUERY, Query, QueryBatch
from nodestream.databases.query_executor import (
    OperationOnNodeIdentity,
    OperationOnRelationshipIdentity,
)
from nodestream.model import (
    Node,
    NodeCreationRule,
    Relationship,
    RelationshipCreationRule,
    RelationshipWithNodes,
    TimeToLiveConfiguration,
)
from nodestream.schema.schema import GraphObjectType


@pytest.fixture
def query_builder():
    return Neo4jIngestQueryBuilder(True)


GREATEST_DAY = datetime(1998, 3, 25, 2, 0, 1)

BASIC_NODE_TTL = TimeToLiveConfiguration(
    graph_object_type=GraphObjectType.NODE,
    object_type="TestNodeType",
    expiry_in_hours=10,
)
BASIC_NODE_TTL_EXPECTED_QUERY = Query(
    COMMIT_QUERY,
    {
        "iterate_params": {"earliest_allowed_time": GREATEST_DAY},
        "batched_query": DELETE_NODE_QUERY,
        "iterable_query": "MATCH (x: TestNodeType) WHERE x.last_ingested_at <= $earliest_allowed_time RETURN id(x) as id",
    },
)

NODE_TTL_WITH_CUSTOM_QUERY = TimeToLiveConfiguration(
    graph_object_type=GraphObjectType.NODE,
    object_type="TestNodeType",
    custom_query="MATCH (n:TestNodeType) RETURN n",
    expiry_in_hours=10,
)
NODE_TTL_WITH_CUSTOM_QUERY_EXPECTED_QUERY = Query(
    COMMIT_QUERY,
    {
        "iterate_params": {"earliest_allowed_time": GREATEST_DAY},
        "batched_query": DELETE_NODE_QUERY,
        "iterable_query": NODE_TTL_WITH_CUSTOM_QUERY.custom_query,
    },
)

BASIC_REL_TTL = TimeToLiveConfiguration(
    graph_object_type=GraphObjectType.RELATIONSHIP,
    object_type="IS_RELATED_TO",
    expiry_in_hours=10,
)
BASIC_REL_TTL_EXPECTED_QUERY = Query(
    COMMIT_QUERY,
    {
        "iterate_params": {"earliest_allowed_time": GREATEST_DAY},
        "iterable_query": "MATCH ()-[x: IS_RELATED_TO]->() WHERE x.last_ingested_at <= $earliest_allowed_time RETURN id(x) as id",
        "batched_query": DELETE_REL_QUERY,
    },
)

REL_TTL_WITH_CUSTOM_QUERY = TimeToLiveConfiguration(
    graph_object_type=GraphObjectType.RELATIONSHIP,
    object_type="IS_RELATED_TO",
    custom_query="MATCH ()-[x: IS_RELATED_TO]->() RETURN id(x) as id",
    expiry_in_hours=10,
)
REL_TTL_WITH_CUSTOM_QUERY_EXPECTED_QUERY = Query(
    COMMIT_QUERY,
    {
        "iterate_params": {"earliest_allowed_time": GREATEST_DAY},
        "iterable_query": REL_TTL_WITH_CUSTOM_QUERY.custom_query,
        "batched_query": DELETE_REL_QUERY,
    },
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

SIMPLE_NODE_EXPECTED_QUERY_ON_MATCH = QueryBatch(
    "MATCH (node: TestType) WHERE node.id = params.__node_id SET node += params.__node_properties",
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
    "MERGE (node: ComplexType {id : params.__node_id}) WITH node, params CALL apoc.create.addLabels(node, params.__node_additional_labels) yield node as _ SET node += params.__node_properties",
    [
        {
            "__node_id": "foo",
            "__node_properties": COMPLEX_NODE.properties,
            "__node_additional_labels": ("ExtraTypeOne", "ExtraTypeTwo"),
        }
    ],
)

COMPLEX_NODE_TWO = Node(
    "ComplexType",
    {"id_part1": "foo", "id_part2": "bar"},
    additional_types=("ExtraTypeOne", "ExtraTypeTwo"),
)

COMPLEX_NODE_TWO_EXPECTED_QUERY = QueryBatch(
    "MERGE (node: ComplexType {id_part1 : params.__node_id_part1, id_part2 : params.__node_id_part2}) WITH node, params CALL apoc.create.addLabels(node, params.__node_additional_labels) yield node as _ SET node += params.__node_properties",
    [
        {
            "__node_id_part1": "foo",
            "__node_id_part2": "bar",
            "__node_properties": COMPLEX_NODE_TWO.properties,
            "__node_additional_labels": ("ExtraTypeOne", "ExtraTypeTwo"),
        }
    ],
)


@pytest.mark.parametrize(
    "node,expected_query,node_creation_rule",
    [
        [SIMPLE_NODE, SIMPLE_NODE_EXPECTED_QUERY, NodeCreationRule.EAGER],
        [COMPLEX_NODE, COMPLEX_NODE_EXPECTED_QUERY, NodeCreationRule.EAGER],
        [COMPLEX_NODE_TWO, COMPLEX_NODE_TWO_EXPECTED_QUERY, NodeCreationRule.EAGER],
        [SIMPLE_NODE, SIMPLE_NODE_EXPECTED_QUERY_ON_MATCH, NodeCreationRule.MATCH_ONLY],
    ],
)
def test_node_update_generates_expected_queries(
    query_builder, node, expected_query, node_creation_rule
):
    operation = OperationOnNodeIdentity(node.identity_shape, node_creation_rule)
    query = query_builder.generate_batch_update_node_operation_batch(operation, [node])
    assert_that(query, equal_to(expected_query))


RELATIONSHIP_BETWEEN_TWO_NODES = RelationshipWithNodes(
    from_node=SIMPLE_NODE,
    to_node=COMPLEX_NODE,
    relationship=Relationship("RELATED_TO"),
)

RELATIONSHIP_BETWEEN_TWO_NODES_EXPECTED_QUERY = QueryBatch(
    """MATCH (from_node: TestType) WHERE from_node.id = params.__from_node_id MATCH (to_node: ComplexType) WHERE to_node.id = params.__to_node_id
    OPTIONAL MATCH  (from_node)-[rel: RELATED_TO]->(to_node)
    FOREACH (x IN CASE WHEN rel IS NULL THEN [1] ELSE [] END |
        CREATE (from_node)-[rel: RELATED_TO]->(to_node) SET rel += params.__rel_properties)
    FOREACH (i in CASE WHEN rel IS NOT NULL THEN [1] ELSE [] END |
        SET rel += params.__rel_properties)
    """,
    [
        {
            "__from_node_id": "foo",
            "__to_node_id": "foo",
            "__rel_properties": RELATIONSHIP_BETWEEN_TWO_NODES.relationship.properties,
        }
    ],
)

RELATIONSHIP_BETWEEN_TWO_NODES_WITH_MULTI_KEY = RelationshipWithNodes(
    from_node=SIMPLE_NODE,
    to_node=COMPLEX_NODE_TWO,
    relationship=Relationship("RELATED_TO"),
)

RELATIONSHIP_BETWEEN_TWO_NODES_EXPECTED_QUERY_WITH_MULTI_KEY = QueryBatch(
    """MATCH (from_node: TestType) WHERE from_node.id = params.__from_node_id MATCH (to_node: ComplexType) WHERE to_node.id_part1 = params.__to_node_id_part1 AND to_node.id_part2 = params.__to_node_id_part2
    OPTIONAL MATCH  (from_node)-[rel: RELATED_TO]->(to_node)
    FOREACH (x IN CASE WHEN rel IS NULL THEN [1] ELSE [] END |
        CREATE (from_node)-[rel: RELATED_TO]->(to_node) SET rel += params.__rel_properties)
    FOREACH (i in CASE WHEN rel IS NOT NULL THEN [1] ELSE [] END |
        SET rel += params.__rel_properties)
    """,
    [
        {
            "__from_node_id": "foo",
            "__to_node_id_part1": "foo",
            "__to_node_id_part2": "bar",
            "__rel_properties": RELATIONSHIP_BETWEEN_TWO_NODES_WITH_MULTI_KEY.relationship.properties,
        }
    ],
)

RELATIONSHIP_BETWEEN_TWO_NODES_WITH_MULTI_KEY_AND_CREATE = RelationshipWithNodes(
    from_node=SIMPLE_NODE,
    to_node=COMPLEX_NODE_TWO,
    relationship=Relationship("RELATED_TO"),
    relationship_creation_rule=RelationshipCreationRule.CREATE,
)

RELATIONSHIP_BETWEEN_TWO_NODES_EXPECTED_QUERY_WITH_MULTI_KEY_AND_CREATE = QueryBatch(
    """MATCH (from_node: TestType) WHERE from_node.id = params.__from_node_id MATCH (to_node: ComplexType) WHERE to_node.id_part1 = params.__to_node_id_part1 AND to_node.id_part2 = params.__to_node_id_part2
    CREATE (from_node)-[rel: RELATED_TO]->(to_node) SET rel += params.__rel_properties""",
    [
        {
            "__from_node_id": "foo",
            "__to_node_id_part1": "foo",
            "__to_node_id_part2": "bar",
            "__rel_properties": RELATIONSHIP_BETWEEN_TWO_NODES_WITH_MULTI_KEY_AND_CREATE.relationship.properties,
        }
    ],
)


@pytest.mark.parametrize(
    "rel,expected_query",
    [
        [RELATIONSHIP_BETWEEN_TWO_NODES, RELATIONSHIP_BETWEEN_TWO_NODES_EXPECTED_QUERY],
        [
            RELATIONSHIP_BETWEEN_TWO_NODES_WITH_MULTI_KEY,
            RELATIONSHIP_BETWEEN_TWO_NODES_EXPECTED_QUERY_WITH_MULTI_KEY,
        ],
        [
            RELATIONSHIP_BETWEEN_TWO_NODES_WITH_MULTI_KEY_AND_CREATE,
            RELATIONSHIP_BETWEEN_TWO_NODES_EXPECTED_QUERY_WITH_MULTI_KEY_AND_CREATE,
        ],
    ],
)
def test_relationship_update_generates_expected_queries(
    query_builder, rel, expected_query
):
    to_op = OperationOnNodeIdentity(rel.to_node.identity_shape, NodeCreationRule.EAGER)
    from_op = OperationOnNodeIdentity(
        rel.from_node.identity_shape, NodeCreationRule.MATCH_ONLY
    )
    operation = OperationOnRelationshipIdentity(
        from_op,
        to_op,
        rel.relationship.identity_shape,
        relationship_creation_rule=rel.relationship_creation_rule,
    )
    query = query_builder.generate_batch_update_relationship_query_batch(
        operation, [rel]
    )
    assert_that(
        query.query_statement,
        equal_to_ignoring_whitespace(expected_query.query_statement),
    )
    assert_that(
        query.batched_parameter_sets, equal_to(expected_query.batched_parameter_sets)
    )
