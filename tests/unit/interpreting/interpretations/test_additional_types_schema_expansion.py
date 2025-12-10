"""Tests for additional_types schema expansion feature."""

from hamcrest import assert_that, equal_to, has_items

from nodestream.interpreting.interpretations import (
    RelationshipInterpretation,
    SourceNodeInterpretation,
)
from nodestream.schema import Schema, SchemaExpansionCoordinator


def test_source_node_additional_types_expand_schema():
    """Test that additional_types are expanded in the schema with the same structure as the main node."""
    schema = Schema()
    coordinator = SchemaExpansionCoordinator(schema)

    interpretation = SourceNodeInterpretation(
        node_type="Player",
        key={"player_id": "test_id"},
        properties={"name": "test_name", "age": 25},
        additional_types=["Person", "Athlete"],
    )

    interpretation.expand_schema(coordinator)
    coordinator.clear_aliases()

    # Check that all three node types exist
    assert_that(schema.has_node_of_type("Player"), equal_to(True))
    assert_that(schema.has_node_of_type("Person"), equal_to(True))
    assert_that(schema.has_node_of_type("Athlete"), equal_to(True))

    # Check that Person has the same properties as Player
    player_schema = schema.get_node_type_by_name("Player")
    person_schema = schema.get_node_type_by_name("Person")
    athlete_schema = schema.get_node_type_by_name("Athlete")

    assert_that(person_schema.keys, equal_to(player_schema.keys))
    assert_that(athlete_schema.keys, equal_to(player_schema.keys))
    assert_that(
        set(person_schema.properties.keys()),
        equal_to(set(player_schema.properties.keys())),
    )
    assert_that(
        set(athlete_schema.properties.keys()),
        equal_to(set(player_schema.properties.keys())),
    )


def test_source_node_and_relationship_additional_types_create_adjacencies():
    """Test that relationships are created for both main node and additional types."""
    schema = Schema()
    coordinator = SchemaExpansionCoordinator(schema)

    # Create source node with additional types
    source_interpretation = SourceNodeInterpretation(
        node_type="Player",
        key={"player_id": "test_id"},
        additional_types=["Person"],
    )

    # Create relationship
    rel_interpretation = RelationshipInterpretation(
        node_type="Team",
        relationship_type="PLAYS_FOR",
        node_key={"team_id": "test_team"},
    )

    source_interpretation.expand_schema(coordinator)
    rel_interpretation.expand_schema(coordinator)
    coordinator.clear_aliases()

    # Check that adjacencies exist for both Player and Person
    adjacency_pairs = [
        (adj.from_node_type, adj.to_node_type) for adj in schema.adjacencies
    ]

    assert_that(
        adjacency_pairs,
        has_items(
            ("Player", "Team"),
            ("Person", "Team"),
        ),
    )


def test_relationship_node_additional_types_create_adjacencies():
    """Test that related nodes with additional types also get relationships."""
    schema = Schema()
    coordinator = SchemaExpansionCoordinator(schema)

    # Create source node
    source_interpretation = SourceNodeInterpretation(
        node_type="Player",
        key={"player_id": "test_id"},
    )

    # Create relationship with node that has additional types
    rel_interpretation = RelationshipInterpretation(
        node_type="Team",
        relationship_type="PLAYS_FOR",
        node_key={"team_id": "test_team"},
        node_additional_types=["Organization"],
    )

    source_interpretation.expand_schema(coordinator)
    rel_interpretation.expand_schema(coordinator)
    coordinator.clear_aliases()

    # Check that adjacencies exist for both Team and Organization
    adjacency_pairs = [
        (adj.from_node_type, adj.to_node_type) for adj in schema.adjacencies
    ]

    assert_that(
        adjacency_pairs,
        has_items(
            ("Player", "Team"),
            ("Player", "Organization"),
        ),
    )

    # Check that both Team and Organization have the same schema
    team_schema = schema.get_node_type_by_name("Team")
    org_schema = schema.get_node_type_by_name("Organization")

    assert_that(
        set(team_schema.properties.keys()), equal_to(set(org_schema.properties.keys()))
    )


def test_both_source_and_related_node_have_additional_types():
    """Test the full feature with both source and related nodes having additional types."""
    schema = Schema()
    coordinator = SchemaExpansionCoordinator(schema)

    # Create source node with additional types
    source_interpretation = SourceNodeInterpretation(
        node_type="Player",
        key={"player_id": "test_id"},
        additional_types=["Person", "Athlete"],
    )

    # Create relationship with node that has additional types
    rel_interpretation = RelationshipInterpretation(
        node_type="Team",
        relationship_type="PLAYS_FOR",
        node_key={"team_id": "test_team"},
        node_additional_types=["Organization"],
    )

    source_interpretation.expand_schema(coordinator)
    rel_interpretation.expand_schema(coordinator)
    coordinator.clear_aliases()

    # Check that all adjacencies exist
    adjacency_pairs = [
        (adj.from_node_type, adj.to_node_type) for adj in schema.adjacencies
    ]

    # Player -> Team, Player -> Organization
    # Person -> Team, Person -> Organization
    # Athlete -> Team, Athlete -> Organization
    assert_that(
        adjacency_pairs,
        has_items(
            ("Player", "Team"),
            ("Player", "Organization"),
            ("Person", "Team"),
            ("Person", "Organization"),
            ("Athlete", "Team"),
            ("Athlete", "Organization"),
        ),
    )
