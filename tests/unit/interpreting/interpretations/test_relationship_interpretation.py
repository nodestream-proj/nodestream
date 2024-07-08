import pytest
from hamcrest import assert_that, equal_to, has_entries, has_length

from nodestream.interpreting.interpretations.relationship_interpretation import (
    InvalidKeyLengthError,
    RelationshipInterpretation,
)
from nodestream.schema import Cardinality
from nodestream.interpreting.interpretations import SourceNodeInterpretation

from ...stubs import StubbedValueProvider
from .matchers import (
    has_defined_relationships,
    has_no_defined_nodes,
    has_no_defined_relationships,
    has_node_keys,
    has_node_properties,
    has_relationship_keys,
    has_unbound_adjacency,
)


def test_single_key_search_without_normalized_values(blank_context):
    expected_key = {"hello": "world", "foo": "bar"}
    RelationshipInterpretation(
        node_type="Test", relationship_type="IS_TEST", node_key=expected_key
    ).interpret(blank_context)
    assert_that(
        blank_context.desired_ingest.relationships[0].to_node.key_values,
        equal_to(expected_key),
    )


def test_single_key_search_with_normalized_values(blank_context):
    provided_key = {"hello": "wOrLd", "foo": "bAr"}
    expected_key = {"hello": "world", "foo": "bar"}
    RelationshipInterpretation(
        node_type="Test", relationship_type="IS_TEST", node_key=provided_key
    ).interpret(blank_context)
    assert_that(
        blank_context.desired_ingest.relationships[0].to_node.key_values,
        equal_to(expected_key),
    )


def test_multiple_key_search_with_normalized_values(blank_context):
    provider_one = StubbedValueProvider(values=[1, 2, 3])
    provider_two = StubbedValueProvider(values=["A", "b", "c"])
    subject = RelationshipInterpretation(
        node_type="Test",
        relationship_type="IS_TEST",
        node_key={"one": provider_one, "two": provider_two},
        find_many=True,
    )

    subject.interpret(blank_context)

    assert_that(blank_context.desired_ingest.relationships, has_length(3))
    assert_that(
        blank_context.desired_ingest.relationships[0].to_node.key_values,
        equal_to({"one": 1, "two": "a"}),
    )


def test_multiple_key_search_without_normalized_values(blank_context):
    provider_one = StubbedValueProvider(values=[1, 2, 3])
    provider_two = StubbedValueProvider(values=["A", "b", "c"])
    subject = RelationshipInterpretation(
        node_type="Test",
        relationship_type="IS_TEST",
        node_key={"one": provider_one, "two": provider_two},
        find_many=True,
        key_normalization={"do_lowercase_strings": False},
    )

    subject.interpret(blank_context)

    assert_that(blank_context.desired_ingest.relationships, has_length(3))
    assert_that(
        blank_context.desired_ingest.relationships[0].to_node.key_values,
        equal_to({"one": 1, "two": "A"}),
    )


def test_multiple_key_search_incongruent_sizes(blank_context):
    with pytest.raises(InvalidKeyLengthError):
        provider_one = StubbedValueProvider(values=[1, 3])
        provider_two = StubbedValueProvider(values=["A", "b", "c"])
        RelationshipInterpretation(
            node_type="Test",
            relationship_type="IS_TEST",
            node_key={"one": provider_one, "two": provider_two},
            find_many=True,
        ).interpret(blank_context)


def test_relationship_interpretation_find_relationship_key_normalization(blank_context):
    expected_key = {"rel_key": "value"}
    RelationshipInterpretation(
        node_type="Test",
        relationship_type="IS_TEST",
        node_key={"hello": "world"},
        relationship_key=expected_key,
    ).interpret(blank_context)
    assert_that(
        blank_context.desired_ingest.relationships[0].relationship.key_values,
        equal_to(expected_key),
    )


def test_relationship_interpretation_find_relationship_property_normalization(
    blank_context,
):
    expected_properties = {"rel_property": "value"}
    supplied_properties = {"rel_property": "VaLuE"}
    RelationshipInterpretation(
        node_type="Test",
        relationship_type="IS_TEST",
        node_key={"hello": "world"},
        relationship_properties=supplied_properties,
        properties_normalization={"do_lowercase_strings": True},
    ).interpret(blank_context)
    assert_that(
        blank_context.desired_ingest.relationships[0].relationship.properties,
        has_entries(expected_properties),
    )


def test_relationship_interpretation_node_property_normalization(blank_context):
    expected_properties = {"rel_property": "value"}
    supplied_properties = {"rel_property": "VaLuE"}
    RelationshipInterpretation(
        node_type="Test",
        relationship_type="IS_TEST",
        node_key={"hello": "world"},
        node_properties=supplied_properties,
        properties_normalization={"do_lowercase_strings": True},
    ).interpret(blank_context)
    assert_that(
        blank_context.desired_ingest.relationships[0].to_node.properties,
        has_entries(expected_properties),
    )


def test_relationship_interpretation_gather_present_relationships_not_static_node(
    schema_coordinator,
):
    subject = RelationshipInterpretation(
        node_type=StubbedValueProvider(values=["Dynamic"]),
        node_key={"hello": "world"},
        relationship_type="Static",
    )
    subject.expand_schema(schema_coordinator)
    assert_that(schema_coordinator, has_defined_relationships(("Static",)))


def test_relationship_interpretation_gather_present_relationships_not_static_relationship(
    schema_coordinator,
):
    subject = RelationshipInterpretation(
        node_type="Satic",
        node_key={"hello": "world"},
        relationship_type=StubbedValueProvider(values=["Dynamic"]),
    )
    subject.expand_schema(schema_coordinator)
    assert_that(schema_coordinator, has_no_defined_relationships())


def test_relationship_interpretation_gather_used_indexes_static_values(
    schema_coordinator,
):
    subject = RelationshipInterpretation(
        node_type=StubbedValueProvider(values=["Dynamic"]),
        node_key={"hello": "world"},
        relationship_type="Static",
    )
    subject.expand_schema(schema_coordinator)
    assert_that(schema_coordinator, has_no_defined_nodes())


def test_relationship_interpretation_gather_used_indexes_static_values_match_only(
    schema_coordinator,
):
    subject = RelationshipInterpretation(
        node_type="static",
        node_key={"hello": "world"},
        relationship_type=StubbedValueProvider(values=["Dynamic"]),
        node_creation_rule="MATCH_ONLY",
    )
    subject.expand_schema(schema_coordinator)
    assert_that(schema_coordinator, has_node_properties("static", ("hello",)))


def test_relationship_interpretation_gather_used_indexes_not_static_node(
    schema_coordinator,
):
    subject = RelationshipInterpretation(
        node_type="Static",
        node_key={"hello": "world"},
        relationship_type="AlsoStatic",
    )
    subject.expand_schema(schema_coordinator)
    assert_that(schema_coordinator, has_node_keys("Static", ("hello",)))
    assert_that(schema_coordinator, has_relationship_keys("AlsoStatic", []))


def test_relationship_interpretation_gather_used_indexes_static_node_reversed(
    schema_coordinator,
):
    subject = RelationshipInterpretation(
        node_type="Static",
        node_key={"hello": "world"},
        relationship_type="AlsoStatic",
        outbound=False,
    )
    subject.expand_schema(schema_coordinator)
    assert_that(schema_coordinator, has_node_keys("Static", ("hello",)))
    assert_that(schema_coordinator, has_relationship_keys("AlsoStatic", []))


def test_relationship_interpretation_gather_object_shapes_not_static_relationship(
    schema_coordinator,
):
    subject = RelationshipInterpretation(
        node_type="Static",
        node_key={"hello": "world"},
        relationship_type=StubbedValueProvider(values=["Dynamic"]),
    )
    subject.expand_schema(schema_coordinator)
    assert_that(schema_coordinator, has_no_defined_relationships())
    assert_that(schema_coordinator, has_node_keys("Static", ("hello",)))


def test_relationship_interpretation_gather_object_shapes_not_static_node(
    schema_coordinator,
):
    subject = RelationshipInterpretation(
        node_type=StubbedValueProvider(values=["Dynamic"]),
        node_key={"hello": "world"},
        relationship_type="Static",
    )
    subject.expand_schema(schema_coordinator)
    assert_that(schema_coordinator, has_no_defined_nodes())


def test_relationship_interpretation_generates_cardinality_based_on_iteration_parameters(
    schema_coordinator,
):
    subject = RelationshipInterpretation(
        node_type="Static",
        node_key={"hello": "world"},
        relationship_type="Static",
        iterate_on=StubbedValueProvider(values=["thing1", "thing2"]),
        find_many=True,
    )
    subject.expand_schema(schema_coordinator)
    assert_that(
        schema_coordinator,
        has_unbound_adjacency(
            from_node_type_or_alias=SourceNodeInterpretation.SOURCE_NODE_TYPE_ALIAS,
            to_node_type_or_alias="Static",
            relationship_type="Static",
            from_node_cardinality=Cardinality.MANY,
            to_node_cardinality=Cardinality.MANY,
        ),
    )


def test_relationship_interpretation_generates_cardinality_based_on_lack_of_iteration_parameters(
    schema_coordinator,
):
    subject = RelationshipInterpretation(
        node_type="Static",
        node_key={"hello": "world"},
        relationship_type="Static",
    )
    subject.expand_schema(schema_coordinator)
    assert_that(
        schema_coordinator,
        has_unbound_adjacency(
            from_node_type_or_alias=SourceNodeInterpretation.SOURCE_NODE_TYPE_ALIAS,
            to_node_type_or_alias="Static",
            relationship_type="Static",
            from_node_cardinality=Cardinality.SINGLE,
            to_node_cardinality=Cardinality.MANY,
        ),
    )


def test_relationship_interpretation_generates_cardinality_based_on_manual_declaration(
    schema_coordinator,
):
    subject = RelationshipInterpretation(
        node_type="Static",
        node_key={"hello": "world"},
        relationship_type="Static",
        cardinality="MANY",
    )
    subject.expand_schema(schema_coordinator)
    assert_that(
        schema_coordinator,
        has_unbound_adjacency(
            from_node_type_or_alias=SourceNodeInterpretation.SOURCE_NODE_TYPE_ALIAS,
            to_node_type_or_alias="Static",
            relationship_type="Static",
            from_node_cardinality=Cardinality.MANY,
            to_node_cardinality=Cardinality.MANY,
        ),
    )


def test_relationship_interpretation_addtional_node_types(blank_context):
    subject = RelationshipInterpretation(
        node_type="Static",
        node_key={"hello": "world"},
        relationship_type="Static",
        node_additional_types=["SomethingElse"],
    )
    subject.interpret(blank_context)
    assert_that(
        blank_context.desired_ingest.relationships[0].to_node.additional_types,
        equal_to(("SomethingElse",)),
    )


def test_relationship_interpretation_with_properties_from_value_provider(blank_context):
    subject = RelationshipInterpretation(
        node_type="Static",
        node_key={"hello": "world"},
        relationship_type="Static",
        relationship_properties=StubbedValueProvider(values=[{"prop": "value"}]),
    )
    subject.interpret(blank_context)
    assert_that(
        blank_context.desired_ingest.relationships[0].relationship.properties,
        has_entries({"prop": "value"}),
    )


def test_relationship_interpretation_with_properties_from_value_provider_wrong_type(
    blank_context,
):
    with pytest.raises(ValueError):
        subject = RelationshipInterpretation(
            node_type="Static",
            node_key={"hello": "world"},
            relationship_type="Static",
            relationship_properties=StubbedValueProvider(values="not a dict"),
        )
        subject.interpret(blank_context)
