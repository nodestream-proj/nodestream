from dataclasses import dataclass

from hamcrest import assert_that, equal_to

from nodestream.schema.state import NodeType, RelationshipType, FieldIndex
from nodestream.schema.migrations.operations import (
    Operation,
    CreateNodeType,
    CreateRelationshipType,
    DropNodeType,
    DropRelationshipType,
    RenameNodeType,
    RenameRelationshipType,
    RenameNodeProperty,
    RenameRelationshipProperty,
    AddNodeProperty,
    AddRelationshipProperty,
    DropNodeProperty,
    DropRelationshipProperty,
    AddAdditionalNodePropertyIndex,
    AddAdditionalRelationshipPropertyIndex,
    DropAdditionalNodePropertyIndex,
    DropAdditionalRelationshipPropertyIndex,
    NodeKeyExtended,
    NodeKeyPartRenamed,
    RelationshipKeyExtended,
    RelationshipKeyPartRenamed,
)


@dataclass
class TestOperation(Operation):
    name: str
    age: int


def test_operation_type_as_snake_case():
    assert_that(
        TestOperation("John", 42).type_as_snake_case(), equal_to("test_operation")
    )


def test_operation_suggest_migration_name_slug():
    assert_that(
        TestOperation("John", 42).suggest_migration_name_slug(),
        equal_to("test_operation"),
    )


def test_from_file_data():
    assert_that(
        TestOperation.validate_and_load(
            {"operation": "TestOperation", "arguments": {"name": "John", "age": 42}}
        ),
        equal_to(TestOperation(name="John", age=42)),
    )


def test_to_file_data():
    assert_that(
        TestOperation(name="John", age=42).to_file_data(),
        equal_to(
            {
                "operation": "TestOperation",
                "arguments": {"name": "John", "age": 42},
            }
        ),
    )


def test_create_node_type_proposed_index_name():
    assert_that(
        CreateNodeType("Person", {"name"}, {}).proposed_index_name,
        equal_to("Person_node_key"),
    )


def test_create_relationship_type_proposed_index_name():
    assert_that(
        CreateRelationshipType("KNOWS", {"since"}, {}).proposed_index_name,
        equal_to("KNOWS_relationship_key"),
    )


def test_add_additional_node_property_index_proposed_index_name():
    assert_that(
        AddAdditionalNodePropertyIndex("Person", "name").proposed_index_name,
        equal_to("Person_name_additional_index"),
    )


def test_add_additional_relationship_property_index_proposed_index_name():
    assert_that(
        AddAdditionalRelationshipPropertyIndex("KNOWS", "since").proposed_index_name,
        equal_to("KNOWS_since_additional_index"),
    )


def test_create_node_suggest_migration_name_slug():
    assert_that(
        CreateNodeType("Person", {"name"}, {}).suggest_migration_name_slug(),
        equal_to("create_node_type_Person"),
    )


def test_create_relationship_suggest_migration_name_slug():
    assert_that(
        CreateRelationshipType("KNOWS", {"since"}, {}).suggest_migration_name_slug(),
        equal_to("create_relationship_type_KNOWS"),
    )


def test_drop_node_type_suggest_migration_name_slug():
    assert_that(
        DropNodeType("Person").suggest_migration_name_slug(),
        equal_to("drop_node_Person"),
    )


def test_drop_relationship_type_suggest_migration_name_slug():
    assert_that(
        DropRelationshipType("KNOWS").suggest_migration_name_slug(),
        equal_to("drop_relationship_KNOWS"),
    )


def test_rename_node_type_suggest_migration_name_slug():
    assert_that(
        RenameNodeType("Person", "Human").suggest_migration_name_slug(),
        equal_to("rename_node_type_Person_to_Human"),
    )


def test_rename_relationship_type_suggest_migration_name_slug():
    assert_that(
        RenameRelationshipType("KNOWS", "LIKES").suggest_migration_name_slug(),
        equal_to("rename_relationship_type_KNOWS_to_LIKES"),
    )


def test_rename_node_property_suggest_migration_name_slug():
    assert_that(
        RenameNodeProperty("Person", "name", "full_name").suggest_migration_name_slug(),
        equal_to("rename_Person_name_to_full_name"),
    )


def test_rename_relationship_property_suggest_migration_name_slug():
    assert_that(
        RenameRelationshipProperty(
            "KNOWS", "since", "known_since"
        ).suggest_migration_name_slug(),
        equal_to("rename_KNOWS_since_to_known_since"),
    )


def test_add_node_property_suggest_migration_name_slug():
    assert_that(
        AddNodeProperty("Person", "full_name").suggest_migration_name_slug(),
        equal_to("add_property_Person_full_name"),
    )


def test_add_relationship_property_suggest_migration_name_slug():
    assert_that(
        AddRelationshipProperty("KNOWS", "known_since").suggest_migration_name_slug(),
        equal_to("add_property_KNOWS_known_since"),
    )


def test_drop_node_property_suggest_migration_name_slug():
    assert_that(
        DropNodeProperty("Person", "full_name").suggest_migration_name_slug(),
        equal_to("drop_property_Person_full_name"),
    )


def test_drop_relationship_property_suggest_migration_name_slug():
    assert_that(
        DropRelationshipProperty("KNOWS", "known_since").suggest_migration_name_slug(),
        equal_to("drop_property_KNOWS_known_since"),
    )


def test_add_additional_node_property_index_suggest_migration_name_slug():
    assert_that(
        AddAdditionalNodePropertyIndex(
            "Person", "full_name"
        ).suggest_migration_name_slug(),
        equal_to("add_index_Person_full_name"),
    )


def test_add_additional_relationship_property_index_suggest_migration_name_slug():
    assert_that(
        AddAdditionalRelationshipPropertyIndex(
            "KNOWS", "known_since"
        ).suggest_migration_name_slug(),
        equal_to("add_index_KNOWS_known_since"),
    )


def test_drop_additional_node_property_index_suggest_migration_name_slug():
    assert_that(
        DropAdditionalNodePropertyIndex(
            "Person", "full_name"
        ).suggest_migration_name_slug(),
        equal_to("drop_index_Person_full_name"),
    )


def test_drop_additional_relationship_property_index_suggest_migration_name_slug():
    assert_that(
        DropAdditionalRelationshipPropertyIndex(
            "KNOWS", "known_since"
        ).suggest_migration_name_slug(),
        equal_to("drop_index_KNOWS_known_since"),
    )


def test_node_key_extended_suggest_migration_name_slug():
    assert_that(
        NodeKeyExtended("Person", "age", 42).suggest_migration_name_slug(),
        equal_to("extend_key_Person_age"),
    )


def test_node_key_part_renamed_suggest_migration_name_slug():
    assert_that(
        NodeKeyPartRenamed("Person", "age", "years").suggest_migration_name_slug(),
        equal_to("rename_key_part_Person_age_to_years"),
    )


def test_relationship_key_extended_suggest_migration_name_slug():
    assert_that(
        RelationshipKeyExtended("KNOWS", "since", 42).suggest_migration_name_slug(),
        equal_to("extend_key_KNOWS_since"),
    )


def test_relationship_key_part_renamed_suggest_migration_name_slug():
    assert_that(
        RelationshipKeyPartRenamed(
            "KNOWS", "since", "known_since"
        ).suggest_migration_name_slug(),
        equal_to("rename_key_part_KNOWS_since_to_known_since"),
    )


def test_create_node_type_as_node_type():
    assert_that(
        CreateNodeType("Person", {"name"}, {"age"}).as_node_type(),
        NodeType("Person", {"name"}, {"age"}, {}),
    )


def test_create_relationship_type_as_relationship_type():
    assert_that(
        CreateRelationshipType("KNOWS", {"since"}, {"since"}).as_relationship_type(),
        RelationshipType("KNOWS", {"since"}, {"since"}, {}),
    )


def test_add_additional_node_property_index_as_index():
    assert_that(
        AddAdditionalNodePropertyIndex("Person", "name").as_index(),
        FieldIndex("name"),
    )


def test_add_additional_relationship_property_index_as_index():
    assert_that(
        AddAdditionalRelationshipPropertyIndex("KNOWS", "since").as_index(),
        FieldIndex("since"),
    )
