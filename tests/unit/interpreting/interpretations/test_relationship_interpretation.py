import pytest
from hamcrest import assert_that, equal_to, has_entries, has_length

from nodestream.interpreting.interpretations.relationship_interpretation import (
    InvalidKeyLengthError,
    RelationshipInterpretation,
)
from nodestream.schema.schema import (
    Cardinality,
    KnownTypeMarker,
    PresentRelationship,
    UnknownTypeMarker,
)

from ...stubs import StubbedValueProvider


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


def test_relationship_interpretation_gather_present_relationships_not_static_node():
    subject = RelationshipInterpretation(
        node_type=StubbedValueProvider(values=["Dynamic"]),
        node_key={"hello": "world"},
        relationship_type="Static",
    )
    assert_that(list(subject.gather_present_relationships()), has_length(0))


def test_relationship_interpretation_gather_present_relationships_not_static_relationship():
    subject = RelationshipInterpretation(
        node_type="Satic",
        node_key={"hello": "world"},
        relationship_type=StubbedValueProvider(values=["Dynamic"]),
    )
    assert_that(list(subject.gather_present_relationships()), has_length(0))


def test_relationship_interpretation_gather_present_relationships_static_values():
    node_type, relationship_type = "Static", "IS_STATIC"
    subject = RelationshipInterpretation(
        node_type=node_type,
        relationship_type=relationship_type,
        node_key={"key": "value"},
        find_many=True,
    )
    result = next(subject.gather_present_relationships())
    assert_that(
        result,
        equal_to(
            PresentRelationship(
                from_object_type=UnknownTypeMarker.source_node(),
                to_object_type=KnownTypeMarker(node_type),
                relationship_type=KnownTypeMarker(relationship_type),
                from_side_cardinality=Cardinality.MANY,
                to_side_cardinality=Cardinality.MANY,
            )
        ),
    )


def test_relationship_interpretation_gather_present_relationships_reverse_direction():
    node_type, relationship_type = "Static", "IS_STATIC"
    subject = RelationshipInterpretation(
        node_type=node_type,
        relationship_type=relationship_type,
        node_key={"key": "value"},
        outbound=True,
    )
    result = next(subject.gather_present_relationships())
    assert_that(
        result,
        equal_to(
            PresentRelationship(
                from_object_type=UnknownTypeMarker.source_node(),
                to_object_type=KnownTypeMarker(node_type),
                relationship_type=KnownTypeMarker(relationship_type),
                from_side_cardinality=Cardinality.SINGLE,
                to_side_cardinality=Cardinality.MANY,
            )
        ),
    )


def test_relationship_interpretation_gather_used_indexes_not_static_relationship():
    subject = RelationshipInterpretation(
        node_type="Satic",
        node_key={"hello": "world"},
        relationship_type=StubbedValueProvider(values=["Dynamic"]),
    )
    assert_that(list(subject.gather_used_indexes()), has_length(2))


def test_relationship_interpretation_gather_used_indexes_static_values():
    subject = RelationshipInterpretation(
        node_type=StubbedValueProvider(values=["Dynamic"]),
        node_key={"hello": "world"},
        relationship_type="Static",
    )
    assert_that(list(subject.gather_used_indexes()), has_length(1))


def test_relationship_interpretation_gather_used_indexes_static_values_match_only():
    subject = RelationshipInterpretation(
        node_type="static",
        node_key={"hello": "world"},
        relationship_type=StubbedValueProvider(values=["Dynamic"]),
        match_strategy="MATCH_ONLY",
    )
    assert_that(list(subject.gather_used_indexes()), has_length(1))


def test_relationship_interpretation_gather_used_indexes_not_static_node():
    subject = RelationshipInterpretation(
        node_type="Static",
        node_key={"hello": "world"},
        relationship_type="AlsoStatic",
    )
    assert_that(list(subject.gather_used_indexes()), has_length(3))


def test_relationship_interpretation_gather_object_shapes_not_static_relationship():
    subject = RelationshipInterpretation(
        node_type="Static",
        node_key={"hello": "world"},
        relationship_type=StubbedValueProvider(values=["Dynamic"]),
    )
    assert_that(list(subject.gather_object_shapes()), has_length(1))


def test_relationship_interpretation_gather_object_shapes_not_static_node():
    subject = RelationshipInterpretation(
        node_type=StubbedValueProvider(values=["Dynamic"]),
        node_key={"hello": "world"},
        relationship_type="Static",
    )
    assert_that(list(subject.gather_object_shapes()), has_length(1))


def test_relationship_interpretation_gather_object_shapess_static_values():
    subject = RelationshipInterpretation(
        node_type="Static",
        node_key={"hello": "world"},
        relationship_type="Static",
    )
    assert_that(list(subject.gather_object_shapes()), has_length(2))
