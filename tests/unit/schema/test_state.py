from copy import deepcopy

import pytest
from hamcrest import assert_that, equal_to, has_key, not_

from nodestream.schema import (
    Adjacency,
    AdjacencyCardinality,
    Cardinality,
    GraphObjectSchema,
    PropertyMetadata,
    PropertyType,
    Schema,
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


def test_rename_property_with_invalid_old_name(basic_schema):
    person = basic_schema.get_node_type_by_name("Person")
    with pytest.raises(ValueError):
        person.rename_property("salary", "wage")


def test_rename_key_when_not_key(basic_schema):
    person = basic_schema.get_node_type_by_name("Person")
    with pytest.raises(ValueError):
        person.rename_key("age", "years_old")


def test_add_keys_when_keys_are_already_defined(basic_schema):
    person = basic_schema.get_node_type_by_name("Person")
    with pytest.raises(ValueError):
        person.add_keys(("name", "ssn"))


def test_invalid_merge_wrong_type(basic_schema):
    person = basic_schema.get_node_type_by_name("Person")
    org = basic_schema.get_node_type_by_name("Organization")
    with pytest.raises(ValueError):
        person.merge(org)


def test_invalid_merge_mismatched_keys(basic_schema):
    person = basic_schema.get_node_type_by_name("Person")
    other_person = GraphObjectSchema("Person", {"ssn": PropertyMetadata(is_key=True)})
    with pytest.raises(ValueError):
        person.merge(other_person)


def test_overlapping_property_definitions(basic_schema):
    person = basic_schema.get_node_type_by_name("NewType")
    person.add_index("name")
    person.add_key("name")
    person.add_property("name")

    property_defintion = person.properties["name"]
    assert_that(property_defintion.is_key, equal_to(True))
    assert_that(property_defintion.is_indexed, equal_to(True))
