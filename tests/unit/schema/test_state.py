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
    SchemaExpansionCoordinator,
    UnboundAdjacency,
)


def test_basic_schema_to_and_from_file(basic_schema):
    file_data = basic_schema.to_file_data()
    rebuilt_schema = Schema.validate_and_load(file_data)
    assert_that(rebuilt_schema.nodes_by_name, equal_to(basic_schema.nodes_by_name))
    assert_that(
        rebuilt_schema.relationships_by_name,
        equal_to(basic_schema.relationships_by_name),
    )
    assert_that(
        all(
            (
                adjacency in rebuilt_schema.cardinalities
                and rebuilt_schema.cardinalities[adjacency] == cardinality
            )
            for adjacency, cardinality, in basic_schema.cardinalities.items()
        )
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


def test_merge_with_adjacency_override(basic_schema):
    copy = deepcopy(basic_schema)
    adjacency = Adjacency("Person", "Person", "BEST_FRIEND_OF")
    cardinality = AdjacencyCardinality("MANY", "MANY")
    copy.cardinalities.clear()
    copy.cardinalities[adjacency] = cardinality

    basic_schema.merge(copy)
    assert_that(len(basic_schema.cardinalities), equal_to(2))
    assert_that(basic_schema.cardinalities[adjacency], equal_to(cardinality))


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


def test_register_additional_types_with_include_additional_types_false():
    coordinator = SchemaExpansionCoordinator(Schema(), include_additional_types=False)
    coordinator.register_additional_types("Person", ("NewType",))
    # When additional types are disabled, registrations should be ignored.
    assert_that(coordinator.additional_types_map.effective_items, equal_to({}))


def test_register_additional_types_with_include_additional_types_true():
    coordinator = SchemaExpansionCoordinator(Schema(), include_additional_types=True)
    coordinator.register_additional_types("Person", ("NewType",))
    # With additional types enabled, the effective mapping should include the
    # registered additional type for the main type.
    assert_that(
        coordinator.additional_types_map.effective_items,
        equal_to({"Person": ("NewType",)}),
    )


def test_clear_aliases_with_include_additional_types_false():
    coordinator = SchemaExpansionCoordinator(Schema(), include_additional_types=False)
    coordinator.register_additional_types("Person", ("NewType",))
    coordinator.clear_aliases()
    # Mapping remains empty because registration is ignored when include_additional_types is False.
    assert_that(coordinator.additional_types_map.effective_items, equal_to({}))


def test_clear_aliases_with_include_additional_types_true():
    coordinator = SchemaExpansionCoordinator(Schema(), include_additional_types=True)
    coordinator.register_additional_types("Person", ("NewType",))
    coordinator.clear_aliases()
    # clear_aliases should not remove the additional types mapping; it uses it
    # to expand adjacencies.
    assert_that(
        coordinator.additional_types_map.effective_items,
        equal_to({"Person": ("NewType",)}),
    )


def test_bind_unbound_adjacencies_uses_aliases_and_updates_schema():
    """_bind_unbound_adjacencies should bind via aliases and mutate schema.cardinalities."""
    schema = Schema()
    coordinator = SchemaExpansionCoordinator(schema)

    # Alias "player_alias" → "Player"
    coordinator.aliases["player_alias"] = "Player"

    # Unbound adjacency from alias to concrete type.
    unbound = UnboundAdjacency(
        from_type_or_alias="player_alias",
        to_type_or_alias="Team",
        relationship_type="PLAYS_FOR",
        from_cardinality=Cardinality.SINGLE,
        to_cardinality=Cardinality.MANY,
    )
    coordinator.unbound_adjacencies.append(unbound)

    base_adjacencies = coordinator._bind_unbound_adjacencies()

    # We should have exactly one bound adjacency with the alias resolved.
    assert_that(len(base_adjacencies), equal_to(1))
    adjacency, cardinality = base_adjacencies[0]
    assert_that(adjacency.from_node_type, equal_to("Player"))
    assert_that(adjacency.to_node_type, equal_to("Team"))
    assert_that(adjacency.relationship_type, equal_to("PLAYS_FOR"))
    # And it should have been written into the schema.
    assert_that(schema.cardinalities[adjacency], equal_to(cardinality))


def test_expand_adjacencies_for_additional_types_duplicates_edges():
    """_expand_adjacencies_for_additional_types should create edges for additional types."""
    schema = Schema()
    coordinator = SchemaExpansionCoordinator(schema, include_additional_types=True)

    base_adj = Adjacency("Player", "Team", "PLAYS_FOR")
    base_card = AdjacencyCardinality(Cardinality.SINGLE, Cardinality.MANY)
    schema.add_adjacency(base_adj, base_card)

    # Register additional types for Player in the current context.
    coordinator.additional_types_map["Player"] = ("Person", "Athlete")

    coordinator._expand_adjacencies_for_additional_types([(base_adj, base_card)])

    adjacency_pairs = {
        (adj.from_node_type, adj.to_node_type) for adj in schema.adjacencies
    }
    # Original plus duplicates for additional types.
    expected = {("Player", "Team"), ("Person", "Team"), ("Athlete", "Team")}
    assert_that(expected.issubset(adjacency_pairs), equal_to(True))


def test_expand_properties_for_additional_types_propagates_alias_properties():
    """Alias-level properties should be applied to additional types for the base type."""
    schema = Schema()
    coordinator = SchemaExpansionCoordinator(schema, include_additional_types=True)

    # Base type and additional types exist in the schema.
    schema.put_node_type(GraphObjectSchema("Player"))
    schema.put_node_type(GraphObjectSchema("Person"))
    schema.put_node_type(GraphObjectSchema("Athlete"))

    # Alias "source_node" is bound to "Player".
    coordinator.aliases["source_node"] = "Player"

    # Alias schema carries properties defined via `properties` interpretations.
    alias_schema = GraphObjectSchema(
        name="source_node",
        properties={
            "alias_prop": PropertyMetadata(PropertyType.STRING),
        },
    )
    coordinator.unbound_aliases["source_node"] = alias_schema

    # Register additional types for Player in this context.
    coordinator.additional_types_map["Player"] = ("Person", "Athlete")

    coordinator._expand_properties_for_additional_types()

    person_schema = schema.get_node_type_by_name("Person")
    athlete_schema = schema.get_node_type_by_name("Athlete")

    assert_that("alias_prop" in person_schema.properties, equal_to(True))
    assert_that("alias_prop" in athlete_schema.properties, equal_to(True))
