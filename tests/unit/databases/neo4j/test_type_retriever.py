import pytest
from hamcrest import assert_that, equal_to, has_length
from neo4j.graph import Node as Neo4jNode
from neo4j.graph import Relationship as Neo4jRelationship

from nodestream.databases.neo4j.type_retriever import Neo4jTypeRetriever
from nodestream.model import Node, Relationship


@pytest.fixture
def subject(mocker):
    connector = mocker.Mock()
    return Neo4jTypeRetriever(connector)


def test_map_neo4j_node_to_nodestream_node(subject):
    neo4j_node = Neo4jNode(None, None, None, ("Person", "Employee"), {"name": "John"})
    result = subject.map_neo4j_node_to_nodestream_node(neo4j_node, type="Person")
    assert result == Node(
        type="Person",
        properties={"name": "John"},
        additional_types=("Employee",),
    )


def test_map_neo4j_relationship_to_nodestream_relationship(subject):
    # For reasons passing understanding, the Neo4jRelationship class is not
    # actually the type that you get back. If you look at the constructor for
    # it you will see that it takes not `type`, yet `type` is a property of
    # the class. How could this be? Well, it turns out that the class is subclassed
    # dynamically by the driver. So, we have to create a mock class that has the
    # same properties as the real class.

    class KNOWS(Neo4jRelationship):
        pass

    neo4j_rel = KNOWS(None, None, "KNOWS", {"since": 2019})
    result = subject.map_neo4j_relationship_to_nodestream_relationship(neo4j_rel)
    assert result == Relationship(
        type="KNOWS",
        properties={"since": 2019},
    )


def test_get_node_type_extractor(subject):
    extractor = subject.get_node_type_extractor("Person")
    expected_query = """
MATCH (n:Person)
RETURN n SKIP $offset LIMIT $limit
"""
    assert_that(extractor.query, equal_to(expected_query))


def test_get_relationship_type_extractor(subject):
    extractor = subject.get_relationship_type_extractor("KNOWS")
    expected_query = """
MATCH (a)-[r:KNOWS]->(b)
RETURN a, r, b SKIP $offset LIMIT $limit
"""
    assert_that(extractor.query, equal_to(expected_query))


async def async_generator(*items):
    for item in items:
        yield item


@pytest.mark.asyncio
async def test_get_nodes_of_type(subject, mocker):
    # Stub out the extractor to return specific values and
    # stub the conversion processes. This is a unit test to
    # test the loop itself not the entire process.
    subject.map_neo4j_node_to_nodestream_node = mocker.Mock()
    subject.get_node_type_extractor = mocker.Mock()
    extractor = subject.get_node_type_extractor.return_value
    extractor.extract_records.return_value = async_generator({"n": 1}, {"n": 2})
    results = [r async for r in subject.get_nodes_of_type("Person")]
    assert_that(results, has_length(2))


@pytest.mark.asyncio
async def test_get_relationships_of_type(subject, mocker):
    # Stub out the extractor to return specific values and
    # stub the conversion processes. This is a unit test to
    # test the loop itself not the entire process.
    subject.map_neo4j_node_to_nodestream_node = mocker.Mock()
    subject.map_neo4j_relationship_to_nodestream_relationship = mocker.Mock()
    subject.get_relationship_type_extractor = mocker.Mock()
    extractor = subject.get_relationship_type_extractor.return_value
    extractor.extract_records.return_value = async_generator(
        {"a": 1, "b": 2, "r": 3}, {"a": 3, "b": 4, "r": 5}
    )
    results = [r async for r in subject.get_relationships_of_type("KNOWS")]
    assert_that(results, has_length(2))
