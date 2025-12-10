"""Tests for migration generation with additional_types feature."""

import pytest
from hamcrest import (
    assert_that,
    contains_inanyorder,
    equal_to,
    greater_than,
    has_item,
    has_length,
)

from nodestream.interpreting.interpretations import (
    RelationshipInterpretation,
    SourceNodeInterpretation,
)
from nodestream.schema import Schema, SchemaExpansionCoordinator
from nodestream.schema.migrations import (
    MigrationGraph,
    MigratorInput,
    ProjectMigrations,
)
from nodestream.schema.migrations.operations import (
    AddAdditionalNodePropertyIndex,
    CreateNodeType,
)


@pytest.fixture
def empty_migration_graph():
    """An empty migration graph (starting state with no migrations)."""
    return MigrationGraph.from_iterable([])


@pytest.fixture
def subject(empty_migration_graph, tmp_path):
    """A ProjectMigrations instance for testing."""
    return ProjectMigrations(empty_migration_graph, tmp_path)


@pytest.fixture
def schema_with_additional_types():
    """A schema with a source node that has additional types."""
    schema = Schema()
    coordinator = SchemaExpansionCoordinator(schema)

    # Create source node with additional types
    interpretation = SourceNodeInterpretation(
        node_type="Player",
        key={"player_id": "test_id"},
        properties={"name": "test_name", "age": 25},
        additional_indexes=["name"],  # Index on name property
        additional_types=["Person", "Athlete"],
    )

    interpretation.expand_schema(coordinator)
    coordinator.clear_aliases()

    return schema


@pytest.fixture
def schema_with_relationship_additional_types():
    """A schema with relationships involving additional types."""
    schema = Schema()
    coordinator = SchemaExpansionCoordinator(schema)

    # Source node with additional types
    source_interpretation = SourceNodeInterpretation(
        node_type="Player",
        key={"player_id": "test_id"},
        additional_types=["Person"],
    )

    # Relationship where related node has additional types
    rel_interpretation = RelationshipInterpretation(
        node_type="Team",
        relationship_type="PLAYS_FOR",
        node_key={"team_id": "test_team"},
        node_additional_types=["Organization", "SportsClub"],
    )

    source_interpretation.expand_schema(coordinator)
    rel_interpretation.expand_schema(coordinator)
    coordinator.clear_aliases()

    return schema


@pytest.mark.asyncio
async def test_migration_creates_additional_node_types(
    subject, schema_with_additional_types
):
    """Test that migrations are created for additional node types."""
    migration = await subject.detect_changes(
        MigratorInput(), schema_with_additional_types
    )

    # Should have operations for Player, Person, and Athlete
    assert_that(migration.operations, has_length(greater_than(0)))

    # Find all CreateNodeType operations
    create_node_ops = [
        op for op in migration.operations if isinstance(op, CreateNodeType)
    ]

    # Should have CreateNodeType for all three types
    assert_that(create_node_ops, has_length(3))

    node_names = [op.name for op in create_node_ops]
    assert_that(node_names, contains_inanyorder("Player", "Person", "Athlete"))

    # All three should have the same keys
    for op in create_node_ops:
        assert_that(set(op.keys), equal_to({"player_id"}))

    # All three should have the same properties (keys are separate from properties)
    for op in create_node_ops:
        assert_that(set(op.properties), equal_to({"name", "age", "last_ingested_at"}))


@pytest.mark.asyncio
async def test_migration_creates_indexes_for_additional_types(
    subject, schema_with_additional_types
):
    """Test that index operations are created for additional types."""
    migration = await subject.detect_changes(
        MigratorInput(), schema_with_additional_types
    )

    # Find all AddAdditionalNodePropertyIndex operations
    index_ops = [
        op
        for op in migration.operations
        if isinstance(op, AddAdditionalNodePropertyIndex)
    ]

    # Should have index operations for all three types (Player, Person, Athlete)
    # Each should have indexes on: name, last_ingested_at
    assert_that(index_ops, has_length(greater_than(0)))

    # Check that indexes are created for additional types
    index_node_types = [op.node_type for op in index_ops]
    assert_that(index_node_types, has_item("Person"))
    assert_that(index_node_types, has_item("Athlete"))

    # Check that the name index exists for Person
    person_name_index = next(
        (
            op
            for op in index_ops
            if op.node_type == "Person" and op.field_name == "name"
        ),
        None,
    )
    assert_that(person_name_index is not None, equal_to(True))

    # Check that the name index exists for Athlete
    athlete_name_index = next(
        (
            op
            for op in index_ops
            if op.node_type == "Athlete" and op.field_name == "name"
        ),
        None,
    )
    assert_that(athlete_name_index is not None, equal_to(True))


@pytest.mark.asyncio
async def test_migration_handles_relationship_additional_types(
    subject, schema_with_relationship_additional_types
):
    """Test that migrations handle additional types on related nodes."""
    migration = await subject.detect_changes(
        MigratorInput(), schema_with_relationship_additional_types
    )

    create_node_ops = [
        op for op in migration.operations if isinstance(op, CreateNodeType)
    ]

    node_names = [op.name for op in create_node_ops]

    # Should create nodes for: Player, Person (additional), Team, Organization (additional), SportsClub (additional)
    assert_that(
        node_names,
        contains_inanyorder("Player", "Person", "Team", "Organization", "SportsClub"),
    )

    # Team, Organization, and SportsClub should all have same keys
    team_ops = [
        op
        for op in create_node_ops
        if op.name in ("Team", "Organization", "SportsClub")
    ]
    for op in team_ops:
        assert_that(set(op.keys), equal_to({"team_id"}))


@pytest.mark.asyncio
async def test_migration_merges_overlapping_additional_types(subject):
    """Test that migrations handle types that appear both as main and additional types."""
    # Start with Player -> Person (additional)
    schema1 = Schema()
    coord1 = SchemaExpansionCoordinator(schema1)

    source1 = SourceNodeInterpretation(
        node_type="Player",
        key={"player_id": "test_id"},
        properties={"name": "test"},
        additional_types=["Person"],
    )
    source1.expand_schema(coord1)
    coord1.clear_aliases()

    # Now Person becomes main type with additional properties
    schema2 = Schema()
    coord2 = SchemaExpansionCoordinator(schema2)

    source1_copy = SourceNodeInterpretation(
        node_type="Player",
        key={"player_id": "test_id"},
        properties={"name": "test"},
        additional_types=["Person"],
    )

    source2 = SourceNodeInterpretation(
        node_type="Person",
        key={"player_id": "test_id"},  # Same key!
        properties={"age": 30},  # Additional property
    )

    source1_copy.expand_schema(coord2)
    source2.expand_schema(coord2)
    coord2.clear_aliases()

    # Generate migration from schema1 to schema2
    migration = await subject.detect_changes(MigratorInput(), schema2)

    # Person should have properties from both contexts
    create_node_ops = [
        op for op in migration.operations if isinstance(op, CreateNodeType)
    ]

    person_op = next((op for op in create_node_ops if op.name == "Person"), None)
    assert_that(person_op is not None, equal_to(True))

    # Person should have both 'name' (from Player) and 'age' (from Person definition)
    assert_that("name" in person_op.properties, equal_to(True))
    assert_that("age" in person_op.properties, equal_to(True))


@pytest.mark.asyncio
async def test_migration_with_multiple_passes(subject):
    """Test migration with complex multi-pass additional types scenario."""
    schema = Schema()
    coordinator = SchemaExpansionCoordinator(schema)

    # Pass 1: Player with Person and Athlete as additional
    pass1 = SourceNodeInterpretation(
        node_type="Player",
        key={"player_id": "id1"},
        properties={"name": "test", "score": 100},
        additional_types=["Person", "Athlete"],
    )

    # Pass 2: Person as main with Human as additional
    pass2 = SourceNodeInterpretation(
        node_type="Person",
        key={"player_id": "id1"},
        properties={"birth_year": 1990},
        additional_types=["Human"],
    )

    # Pass 3: Athlete as main with Competitor as additional
    pass3 = SourceNodeInterpretation(
        node_type="Athlete",
        key={"player_id": "id1"},
        properties={"sport": "football"},
        additional_types=["Competitor"],
    )

    pass1.expand_schema(coordinator)
    pass2.expand_schema(coordinator)
    pass3.expand_schema(coordinator)
    coordinator.clear_aliases()

    migration = await subject.detect_changes(MigratorInput(), schema)

    create_node_ops = [
        op for op in migration.operations if isinstance(op, CreateNodeType)
    ]

    node_names = [op.name for op in create_node_ops]

    # Should have all node types
    assert_that(
        node_names,
        contains_inanyorder("Player", "Person", "Athlete", "Human", "Competitor"),
    )

    # Verify merged properties
    person_op = next((op for op in create_node_ops if op.name == "Person"), None)
    assert_that("name" in person_op.properties, equal_to(True))  # From Player
    assert_that("score" in person_op.properties, equal_to(True))  # From Player
    assert_that("birth_year" in person_op.properties, equal_to(True))  # From Person

    athlete_op = next((op for op in create_node_ops if op.name == "Athlete"), None)
    assert_that("name" in athlete_op.properties, equal_to(True))  # From Player
    assert_that("score" in athlete_op.properties, equal_to(True))  # From Player
    assert_that("sport" in athlete_op.properties, equal_to(True))  # From Athlete
