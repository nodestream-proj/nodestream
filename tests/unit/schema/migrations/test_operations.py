from dataclasses import dataclass

from hamcrest import assert_that, equal_to

from nodestream.schema.migrations.operations import (
    AddAdditionalNodePropertyIndex,
    AddAdditionalRelationshipPropertyIndex,
    AddNodeProperty,
    AddRelationshipProperty,
    CreateNodeType,
    CreateRelationshipType,
    DropAdditionalNodePropertyIndex,
    DropAdditionalRelationshipPropertyIndex,
    DropNodeProperty,
    DropNodeType,
    DropRelationshipProperty,
    DropRelationshipType,
    NodeKeyExtended,
    NodeKeyPartRenamed,
    Operation,
    RelationshipKeyExtended,
    RelationshipKeyPartRenamed,
    RenameNodeProperty,
    RenameNodeType,
    RenameRelationshipProperty,
    RenameRelationshipType,
)
from nodestream.schema.state import GraphObjectSchema, PropertyMetadata


@dataclass
class DummyOperation(Operation):
    name: str
    age: int


def test_operation_type_as_snake_case():
    assert_that(
        DummyOperation("John", 42).type_as_snake_case(), equal_to("dummy_operation")
    )


def test_operation_suggest_migration_name_slug():
    assert_that(
        DummyOperation("John", 42).suggest_migration_name_slug(),
        equal_to("dummy_operation"),
    )


def test_from_file_data():
    assert_that(
        DummyOperation.validate_and_load(
            {"operation": "DummyOperation", "arguments": {"name": "John", "age": 42}}
        ),
        equal_to(DummyOperation(name="John", age=42)),
    )


def test_to_file_data():
    assert_that(
        DummyOperation(name="John", age=42).to_file_data(),
        equal_to(
            {
                "operation": "DummyOperation",
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
        equal_to("create_node_type_person"),
    )


def test_create_relationship_suggest_migration_name_slug():
    assert_that(
        CreateRelationshipType("KNOWS", {"since"}, {}).suggest_migration_name_slug(),
        equal_to("create_relationship_type_knows"),
    )


def test_drop_node_type_suggest_migration_name_slug():
    assert_that(
        DropNodeType("Person").suggest_migration_name_slug(),
        equal_to("drop_node_type_person"),
    )


def test_drop_relationship_type_suggest_migration_name_slug():
    assert_that(
        DropRelationshipType("KNOWS").suggest_migration_name_slug(),
        equal_to("drop_relationship_knows"),
    )


def test_rename_node_type_suggest_migration_name_slug():
    assert_that(
        RenameNodeType("Person", "Human").suggest_migration_name_slug(),
        equal_to("rename_node_type_person_to_human"),
    )


def test_rename_relationship_type_suggest_migration_name_slug():
    assert_that(
        RenameRelationshipType("KNOWS", "LIKES").suggest_migration_name_slug(),
        equal_to("rename_relationship_type_knows_to_likes"),
    )


def test_rename_node_property_suggest_migration_name_slug():
    assert_that(
        RenameNodeProperty("Person", "name", "full_name").suggest_migration_name_slug(),
        equal_to("rename_node_property_name_to_full_name_on_node_type_person"),
    )


def test_rename_relationship_property_suggest_migration_name_slug():
    assert_that(
        RenameRelationshipProperty(
            "KNOWS", "since", "known_since"
        ).suggest_migration_name_slug(),
        equal_to(
            "rename_relationship_property_since_to_known_since_on_relationship_type_knows"
        ),
    )


def test_add_node_property_suggest_migration_name_slug():
    assert_that(
        AddNodeProperty("Person", "full_name").suggest_migration_name_slug(),
        equal_to("add_property_full_name_to_node_type_person"),
    )


def test_add_relationship_property_suggest_migration_name_slug():
    assert_that(
        AddRelationshipProperty("KNOWS", "known_since").suggest_migration_name_slug(),
        equal_to("add_property_known_since_to_relationship_type_knows"),
    )


def test_drop_node_property_suggest_migration_name_slug():
    assert_that(
        DropNodeProperty("Person", "full_name").suggest_migration_name_slug(),
        equal_to("drop_property_full_name_from_node_type_person"),
    )


def test_drop_relationship_property_suggest_migration_name_slug():
    assert_that(
        DropRelationshipProperty("KNOWS", "known_since").suggest_migration_name_slug(),
        equal_to("drop_property_known_since_from_relationship_type_knows"),
    )


def test_add_additional_node_property_index_suggest_migration_name_slug():
    assert_that(
        AddAdditionalNodePropertyIndex(
            "Person", "full_name"
        ).suggest_migration_name_slug(),
        equal_to("add_additional_index_for_node_type_person_on_field_full_name"),
    )


def test_add_additional_relationship_property_index_suggest_migration_name_slug():
    assert_that(
        AddAdditionalRelationshipPropertyIndex(
            "KNOWS", "known_since"
        ).suggest_migration_name_slug(),
        equal_to(
            "add_additional_index_for_relationship_type_knows_on_field_known_since"
        ),
    )


def test_drop_additional_node_property_index_suggest_migration_name_slug():
    assert_that(
        DropAdditionalNodePropertyIndex(
            "Person", "full_name"
        ).suggest_migration_name_slug(),
        equal_to("drop_additional_index_for_node_type_person_on_field_full_name"),
    )


def test_drop_additional_relationship_property_index_suggest_migration_name_slug():
    assert_that(
        DropAdditionalRelationshipPropertyIndex(
            "KNOWS", "known_since"
        ).suggest_migration_name_slug(),
        equal_to(
            "drop_additional_index_for_relationship_type_knows_on_field_known_since"
        ),
    )


def test_node_key_extended_suggest_migration_name_slug():
    assert_that(
        NodeKeyExtended("Person", "age", 42).suggest_migration_name_slug(),
        equal_to("extend_key_age_on_node_type_person"),
    )


def test_node_key_part_renamed_suggest_migration_name_slug():
    assert_that(
        NodeKeyPartRenamed("Person", "age", "years").suggest_migration_name_slug(),
        equal_to("rename_key_part_age_to_years_on_node_type_person"),
    )


def test_relationship_key_extended_suggest_migration_name_slug():
    assert_that(
        RelationshipKeyExtended("KNOWS", "since", 42).suggest_migration_name_slug(),
        equal_to("extend_key_since_on_relationship_type_knows"),
    )


def test_relationship_key_part_renamed_suggest_migration_name_slug():
    assert_that(
        RelationshipKeyPartRenamed(
            "KNOWS", "since", "known_since"
        ).suggest_migration_name_slug(),
        equal_to("rename_key_part_since_to_known_since_on_relationship_type_knows"),
    )


def test_create_node_type_as_node_type():
    assert_that(
        CreateNodeType("Person", {"name"}, {"age"}).as_node_type(),
        GraphObjectSchema(
            "Person", {"name": PropertyMetadata(is_key=True), "age": PropertyMetadata()}
        ),
    )


def test_create_relationship_type_as_relationship_type():
    assert_that(
        CreateRelationshipType("KNOWS", {"since"}, {"since"}).as_relationship_type(),
        GraphObjectSchema("KNOWS", {"since": PropertyMetadata(is_key=True)}),
    )


def test_node_key_part_renamed_proposed_index_name():
    name = NodeKeyPartRenamed("Person", "age", "years").proposed_index_name
    assert_that(name, equal_to("Person_node_key"))


def test_node_key_extended_proposed_index_name():
    name = NodeKeyExtended("Person", "name", None).proposed_index_name
    assert_that(name, equal_to("Person_node_key"))


def test_drop_rel_index_proposed_index_name():
    name = DropAdditionalRelationshipPropertyIndex("KNOWS", "since").proposed_index_name
    assert_that(name, equal_to("KNOWS_since_additional_index"))


def test_drop_additional_node_property_index_name():
    name = DropAdditionalNodePropertyIndex("Person", "age").proposed_index_name
    assert_that(name, equal_to("Person_age_additional_index"))


def test_rename_node_type_old_name():
    name = RenameNodeType("Person", "Human").old_proposed_index_name
    assert_that(name, equal_to("Person_node_key"))


def test_rename_node_type_new_name():
    name = RenameNodeType("Person", "Human").new_proposed_index_name
    assert_that(name, equal_to("Human_node_key"))


def test_drop_node_type_index_name():
    name = DropNodeType("Person").proposed_index_name
    assert_that(name, equal_to("Person_node_key"))


def test_create_node_type_reduce():
    a = CreateNodeType("Person", {"name"}, {"age"})
    b = DropNodeType("Person")
    assert_that(a.reduce(b), equal_to([]))


def test_create_node_type_reduce_no_match():
    a = CreateNodeType("Person", {"name"}, {"age"})
    b = DropNodeType("Human")
    assert_that(a.reduce(b), equal_to([a, b]))


def test_create_node_type_reduce_irrelevant_type():
    a = CreateNodeType("Person", {"name"}, {"age"})
    b = AddNodeProperty("Person", "full_name")
    assert_that(a.reduce(b), equal_to([a, b]))


def test_create_relationship_type_reduce():
    a = CreateRelationshipType("KNOWS", {"since"}, {"since"})
    b = DropRelationshipType("KNOWS")
    assert_that(a.reduce(b), equal_to([]))


def test_create_relationship_type_reduce_no_match():
    a = CreateRelationshipType("KNOWS", {"since"}, {"since"})
    b = DropRelationshipType("LIKES")
    assert_that(a.reduce(b), equal_to([a, b]))


def test_create_relationship_type_reduce_irrelevant_type():
    a = CreateRelationshipType("KNOWS", {"since"}, {"since"})
    b = AddRelationshipProperty("KNOWS", "known_since")
    assert_that(a.reduce(b), equal_to([a, b]))


def test_drop_node_type_reduce():
    a = DropNodeType("Person")
    b = CreateNodeType("Person", {"name"}, {"age"})
    assert_that(a.reduce(b), equal_to([]))


def test_drop_node_type_reduce_no_match():
    a = DropNodeType("Person")
    b = CreateNodeType("Human", {"name"}, {"age"})
    assert_that(a.reduce(b), equal_to([a, b]))


def test_drop_node_type_reduce_irrelevant_type():
    a = DropNodeType("Person")
    b = AddNodeProperty("Person", "full_name")
    assert_that(a.reduce(b), equal_to([a]))


def test_drop_relationship_type_reduce():
    a = DropRelationshipType("KNOWS")
    b = CreateRelationshipType("KNOWS", {"since"}, {"since"})
    assert_that(a.reduce(b), equal_to([]))


def test_drop_relationship_type_reduce_no_match():
    a = DropRelationshipType("KNOWS")
    b = CreateRelationshipType("LIKES", {"since"}, {"since"})
    assert_that(a.reduce(b), equal_to([a, b]))


def test_drop_relationship_type_reduce_dropping_added_property():
    a = DropRelationshipType("KNOWS")
    b = AddRelationshipProperty("KNOWS", "known_since")
    assert_that(a.reduce(b), equal_to([a]))


def test_rename_node_property_reduce_add():
    a = RenameNodeProperty("Person", "full_name", "fuller_name")
    b = AddNodeProperty("Person", "full_name")
    assert_that(a.reduce(b), equal_to([AddNodeProperty("Person", "fuller_name")]))


def test_rename_node_property_reduce_drop():
    a = RenameNodeProperty("Person", "full_name", "fuller_name")
    b = DropNodeProperty("Person", "full_name")
    assert_that(a.reduce(b), equal_to([DropNodeProperty("Person", "full_name")]))


def test_rename_node_property_reduce_no_match():
    a = RenameNodeProperty("Person", "full_name", "fuller_name")
    b = AddNodeProperty("Person", "name")
    assert_that(a.reduce(b), equal_to([a, b]))


def test_rename_node_property_reduce_irrelevant_type():
    a = RenameNodeProperty("Person", "full_name", "fuller_name")
    b = AddRelationshipProperty("KNOWS", "known_since")
    assert_that(a.reduce(b), equal_to([a, b]))


def test_rename_relationship_property_reduce_add():
    a = RenameRelationshipProperty("KNOWS", "known_since", "known_since_date")
    b = AddRelationshipProperty("KNOWS", "known_since")
    assert_that(
        a.reduce(b), equal_to([AddRelationshipProperty("KNOWS", "known_since_date")])
    )


def test_rename_relationship_property_reduce_drop():
    a = RenameRelationshipProperty("KNOWS", "known_since", "known_since_date")
    b = DropRelationshipProperty("KNOWS", "known_since")
    assert_that(
        a.reduce(b), equal_to([DropRelationshipProperty("KNOWS", "known_since")])
    )


def test_rename_relationship_property_reduce_no_match():
    a = RenameRelationshipProperty("KNOWS", "known_since", "known_since_date")
    b = AddRelationshipProperty("KNOWS", "since")
    assert_that(a.reduce(b), equal_to([a, b]))


def test_rename_relationship_property_reduce_irrelevant_type():
    a = RenameRelationshipProperty("KNOWS", "known_since", "known_since_date")
    b = AddNodeProperty("Person", "full_name")
    assert_that(a.reduce(b), equal_to([a, b]))


def test_rename_node_type_reduce():
    a = RenameNodeType("Person", "Human")
    b = CreateNodeType("Person", {"name"}, {"age"})
    assert_that(a.reduce(b), equal_to([CreateNodeType("Human", {"name"}, {"age"})]))


def test_rename_node_type_reduce_drop():
    a = RenameNodeType("Person", "Human")
    b = DropNodeType("Human")
    assert_that(a.reduce(b), equal_to([DropNodeType("Person")]))


def test_rename_node_type_reduce_no_match():
    a = RenameNodeType("Person", "Human")
    b = CreateNodeType("Human", {"name"}, {"age"})
    assert_that(a.reduce(b), equal_to([a, b]))


def test_rename_node_type_reduce_irrelevant_type():
    a = RenameNodeType("Person", "Human")
    b = AddNodeProperty("Person", "full_name")
    assert_that(a.reduce(b), equal_to([a, b]))


def test_rename_relationship_type_reduce():
    a = RenameRelationshipType("KNOWS", "LIKES")
    b = CreateRelationshipType("KNOWS", {"since"}, {"since"})
    assert_that(
        a.reduce(b), equal_to([CreateRelationshipType("LIKES", {"since"}, {"since"})])
    )


def test_rename_relationship_type_reduce_drop():
    a = RenameRelationshipType("KNOWS", "LIKES")
    b = DropRelationshipType("LIKES")
    assert_that(a.reduce(b), equal_to([DropRelationshipType("KNOWS")]))


def test_rename_relationship_type_reduce_no_match():
    a = RenameRelationshipType("KNOWS", "LIKES")
    b = CreateRelationshipType("LIKES", {"since"}, {"since"})
    assert_that(a.reduce(b), equal_to([a, b]))


def test_rename_relationship_type_reduce_irrelevant_type():
    a = RenameRelationshipType("KNOWS", "LIKES")
    b = AddNodeProperty("Person", "full_name")
    assert_that(a.reduce(b), equal_to([a, b]))


def test_add_node_property_reduce_drop():
    a = AddNodeProperty("Person", "full_name")
    b = DropNodeProperty("Person", "full_name")
    assert_that(a.reduce(b), equal_to([]))


def test_add_node_property_reduce_rename():
    a = AddNodeProperty("Person", "full_name")
    b = RenameNodeProperty("Person", "full_name", "fuller_name")
    assert_that(a.reduce(b), equal_to([AddNodeProperty("Person", "fuller_name")]))


def test_add_node_property_reduce_no_match():
    a = AddNodeProperty("Person", "full_name")
    b = DropNodeProperty("Person", "name")
    assert_that(a.reduce(b), equal_to([a, b]))


def test_add_node_property_reduce_irrelevant_type():
    a = AddNodeProperty("Person", "full_name")
    b = AddRelationshipProperty("KNOWS", "known_since")
    assert_that(a.reduce(b), equal_to([a, b]))


def test_add_relationship_property_reduce_drop():
    a = AddRelationshipProperty("KNOWS", "known_since")
    b = DropRelationshipProperty("KNOWS", "known_since")
    assert_that(a.reduce(b), equal_to([]))


def test_add_relationship_property_reduce_rename():
    a = AddRelationshipProperty("KNOWS", "known_since")
    b = RenameRelationshipProperty("KNOWS", "known_since", "known_since_date")
    assert_that(
        a.reduce(b), equal_to([AddRelationshipProperty("KNOWS", "known_since_date")])
    )


def test_add_relationship_property_reduce_no_match():
    a = AddRelationshipProperty("KNOWS", "known_since")
    b = DropRelationshipProperty("KNOWS", "since")
    assert_that(a.reduce(b), equal_to([a, b]))


def test_add_relationship_property_reduce_irrelevant_type():
    a = AddRelationshipProperty("KNOWS", "known_since")
    b = AddNodeProperty("Person", "full_name")
    assert_that(a.reduce(b), equal_to([a, b]))


def test_drop_node_property_reduce():
    a = DropNodeProperty("Person", "full_name")
    b = AddNodeProperty("Person", "full_name")
    assert_that(a.reduce(b), equal_to([]))


def test_drop_node_property_reduce_rename():
    a = DropNodeProperty("Person", "full_name")
    b = RenameNodeProperty("Person", "full_name", "fuller_name")
    assert_that(a.reduce(b), equal_to([DropNodeProperty("Person", "full_name")]))


def test_drop_node_property_reduce_no_match():
    a = DropNodeProperty("Person", "full_name")
    b = AddNodeProperty("Person", "name")
    assert_that(a.reduce(b), equal_to([a, b]))


def test_drop_node_property_reduce_irrelevant_type():
    a = DropNodeProperty("Person", "full_name")
    b = AddRelationshipProperty("KNOWS", "known_since")
    assert_that(a.reduce(b), equal_to([a, b]))


def test_drop_relationship_property_reduce():
    a = DropRelationshipProperty("KNOWS", "known_since")
    b = AddRelationshipProperty("KNOWS", "known_since")
    assert_that(a.reduce(b), equal_to([]))


def test_drop_relationship_property_reduce_rename():
    a = DropRelationshipProperty("KNOWS", "known_since")
    b = RenameRelationshipProperty("KNOWS", "known_since", "known_since_date")
    assert_that(
        a.reduce(b), equal_to([DropRelationshipProperty("KNOWS", "known_since")])
    )


def test_drop_relationship_property_reduce_no_match():
    a = DropRelationshipProperty("KNOWS", "known_since")
    b = AddRelationshipProperty("KNOWS", "since")
    assert_that(a.reduce(b), equal_to([a, b]))


def test_drop_relationship_property_reduce_irrelevant_type():
    a = DropRelationshipProperty("KNOWS", "known_since")
    b = AddNodeProperty("Person", "full_name")
    assert_that(a.reduce(b), equal_to([a, b]))


def test_operation_chain_optimization():
    a = CreateNodeType("Person", {"name"}, {"age"})
    b = CreateNodeType("Sport", {"name"}, {})
    c = AddNodeProperty("Person", "full_name")
    d = DropNodeProperty("Person", "age")
    e = DropNodeType("Person")

    assert_that(
        Operation.optimize([a, b, c, d, e]),
        equal_to([CreateNodeType("Sport", {"name"}, {})]),
    )


def test_operation_chain_optimization_complex():
    operations = [
        CreateNodeType("Person", {"name"}, {"age"}),
        CreateRelationshipType("Likes", {}, {}),
        CreateNodeType("Sport", {"name"}, {}),
        AddRelationshipProperty("Likes", "since"),
        AddNodeProperty("Person", "full_name"),
        CreateNodeType("Team", {"name"}, {}),
        DropNodeProperty("Person", "age"),
        DropRelationshipType("Likes"),
        DropNodeProperty("Team", "mascot"),
        DropNodeType("Person"),
        DropRelationshipProperty("Likes", "since"),
        AddNodeProperty("Team", "mascot"),
        DropNodeType("Team"),
    ]

    assert_that(
        Operation.optimize(operations),
        equal_to([CreateNodeType("Sport", {"name"}, {})]),
    )
