from copy import deepcopy

from hamcrest import assert_that, equal_to, has_key, not_

from nodestream.schema import (
    Schema,
    Adjacency,
    AdjacencyCardinality,
    Cardinality,
    PropertyType,
)


def test_basic_schema_to_and_from_file(basic_schema):
    file_data = basic_schema.to_file_data()
    rebuilt_schema = Schema.validate_and_load(file_data)
    assert_that(rebuilt_schema.nodes_by_name, equal_to(basic_schema.nodes_by_name))
    assert_that(
        rebuilt_schema.relationships_by_name,
        equal_to(basic_schema.relationships_by_name),
    )


def test_basic_schema_merge_with_self_should_be_same(basic_schema):
    basic_schema.merge(copy := deepcopy(basic_schema))
    assert_that(basic_schema, equal_to(copy))


def test_basic_schema_merge_with_empty_should_be_same(basic_schema):
    copy = deepcopy(basic_schema)
    basic_schema.merge(Schema())
    assert_that(basic_schema, equal_to(copy))


def test_merge_with_differences(basic_schema):
    copy = deepcopy(basic_schema)
    person = copy.get_node_type_by_name("Person")
    person.properties["age"].type = PropertyType.FLOAT
    person.add_property("new_property")
    copy.get_node_type_by_name("NewNodeType")

    basic_schema.merge(copy)
    assert_that(
        basic_schema.get_node_type_by_name("Person").properties["age"].type,
        equal_to(PropertyType.FLOAT),
    )
    assert_that(basic_schema.has_node_of_type("NewNodeType"), equal_to(True))
    assert_that(
        basic_schema.get_node_type_by_name("Person").properties, has_key("new_property")
    )


def test_has_node_of_type(basic_schema):
    assert_that(basic_schema.has_node_of_type("Person"), equal_to(True))
    assert_that(basic_schema.has_node_of_type("NotPerson"), equal_to(False))


def test_has_relationship_of_type(basic_schema):
    assert_that(basic_schema.has_relationship_of_type("BEST_FRIEND_OF"), equal_to(True))
    assert_that(
        basic_schema.has_relationship_of_type("NOT_BEST_FRIEND_OF"), equal_to(False)
    )


def test_add_drop_adjacency(basic_schema):
    adjacency = Adjacency("Person", "Person", "BEST_FRIEND_OF")
    cardinality = AdjacencyCardinality(Cardinality.SINGLE, Cardinality.MANY)
    basic_schema.add_adjacency(adjacency, cardinality)
    assert_that(basic_schema.cardinalities, has_key(adjacency))
    assert_that(
        basic_schema.get_adjacency_cardinality(adjacency), equal_to(cardinality)
    )
    basic_schema.drop_adjacency(adjacency)
    assert_that(basic_schema.cardinalities, not_(has_key(adjacency)))


def test_has_matching_properties(basic_schema):
    person = basic_schema.get_node_type_by_name("Person")
    organization = basic_schema.get_node_type_by_name("Organization")
    assert_that(person.has_matching_properties(person), equal_to(True))
    assert_that(person.has_matching_properties(organization), equal_to(False))
