from hamcrest import assert_that, equal_to, has_entries, has_length

from nodestream.interpreting.interpretations import SourceNodeInterpretation
from nodestream.schema.schema import (
    GraphObjectShape,
    GraphObjectType,
    KnownTypeMarker,
    PropertyMetadataSet,
)
from nodestream.schema.indexes import KeyIndex, FieldIndex

from ...stubs import StubbedValueProvider

EXPECTED_NODE_TYPE = "Person"


def test_source_node_interpretation_sets_type_statically(blank_context):
    subject = SourceNodeInterpretation(node_type=EXPECTED_NODE_TYPE, key={})
    subject.interpret(blank_context)
    assert_that(blank_context.desired_ingest.source.type, equal_to(EXPECTED_NODE_TYPE))


def test_source_node_interpretation_sets_type_dynamically(blank_context):
    subject = SourceNodeInterpretation(
        node_type=StubbedValueProvider(values=[EXPECTED_NODE_TYPE]), key={}
    )
    subject.interpret(blank_context)
    assert_that(blank_context.desired_ingest.source.type, equal_to(EXPECTED_NODE_TYPE))


def test_source_node_interpretation_applies_static_keys(blank_context):
    expected_keys = {"first_name": "zach", "last_name": "probst"}
    subject = SourceNodeInterpretation(node_type=EXPECTED_NODE_TYPE, key=expected_keys)
    subject.interpret(blank_context)
    assert_that(blank_context.desired_ingest.source.key_values, equal_to(expected_keys))


def test_source_node_interpretation_applies_dynamic_keys(blank_context):
    first_name = "zach"
    expected_keys = {"first_name": first_name, "last_name": "probst"}
    dynamic_first_name = StubbedValueProvider(values=[first_name])
    subject = SourceNodeInterpretation(
        node_type=EXPECTED_NODE_TYPE,
        key={"first_name": dynamic_first_name, "last_name": "probst"},
    )
    subject.interpret(blank_context)
    assert_that(blank_context.desired_ingest.source.key_values, equal_to(expected_keys))


def test_source_node_interpretaton_applies_static_properties(blank_context):
    expected_properties = {"first_name": "zach", "last_name": "probst"}
    subject = SourceNodeInterpretation(
        node_type=EXPECTED_NODE_TYPE, key={}, properties=expected_properties
    )
    subject.interpret(blank_context)
    actual_properties = blank_context.desired_ingest.source.properties
    assert_that(actual_properties, has_entries(expected_properties))


def test_source_node_interpretation_applies_dynamic_properties(blank_context):
    first_name = "zach"
    expected_properties = {"first_name": first_name, "last_name": "probst"}
    dynamic_first_name = StubbedValueProvider(values=[first_name])
    subject = SourceNodeInterpretation(
        node_type=EXPECTED_NODE_TYPE,
        key={},
        properties={"first_name": dynamic_first_name, "last_name": "Probst"},
    )
    subject.interpret(blank_context)
    actual_properties = blank_context.desired_ingest.source.properties
    assert_that(actual_properties, has_entries(expected_properties))


def test_source_node_interpretation_applies_static_additional_types(blank_context):
    customer = "Customer"
    subject = SourceNodeInterpretation(
        node_type=EXPECTED_NODE_TYPE, key={}, additional_types=[customer]
    )
    subject.interpret(blank_context)
    assert_that(
        blank_context.desired_ingest.source.additional_types,
        equal_to(tuple([customer])),
    )


def test_keys_are_lower_cased_by_default(blank_context):
    provided_keys = {"first_name": "Zach", "last_name": "Probst"}
    expected_keys = {"first_name": "zach", "last_name": "probst"}
    subject = SourceNodeInterpretation(node_type=EXPECTED_NODE_TYPE, key=provided_keys)
    subject.interpret(blank_context)
    assert_that(blank_context.desired_ingest.source.key_values, equal_to(expected_keys))


def test_gather_used_indexes_non_static():
    dynamic_type = StubbedValueProvider(values=["zach"])
    subject = SourceNodeInterpretation(node_type=dynamic_type, key={})
    assert_that(list(subject.gather_used_indexes()), has_length(0))


def test_gather_object_shapes_non_static():
    dynamic_type = StubbedValueProvider(values=["zach"])
    subject = SourceNodeInterpretation(node_type=dynamic_type, key={})
    assert_that(list(subject.gather_object_shapes()), has_length(0))


def test_gather_present_relationships():
    subject = SourceNodeInterpretation(node_type="test", key={"first_name": "test"})
    assert_that(list(subject.gather_present_relationships()), has_length(0))


def test_gather_used_indexes_static():
    subject = SourceNodeInterpretation(
        node_type="test",
        key={"foo": 1, "bar": False},
        additional_indexes=("baz",),
        properties={"baz": False},
    )

    key_index, timestamp_index, field_index = subject.gather_used_indexes()
    expected_key_index = KeyIndex("test", frozenset(("foo", "bar")))
    expected_ts_index = FieldIndex("test", "last_ingested_at", GraphObjectType.NODE)
    expected_field_index = FieldIndex("test", "baz", GraphObjectType.NODE)

    assert_that(key_index, equal_to(expected_key_index))
    assert_that(timestamp_index, equal_to(expected_ts_index))
    assert_that(field_index, equal_to(expected_field_index))


def test_gather_object_shapes():
    subject = SourceNodeInterpretation(
        node_type="test",
        key={"foo": 1, "bar": False},
        properties={"baz": False},
    )
    object_shape = next(subject.gather_object_shapes())
    expected_object_shape = GraphObjectShape(
        graph_object_type=GraphObjectType.NODE,
        object_type=KnownTypeMarker.fulfilling_source_node("test"),
        properties=PropertyMetadataSet.from_names(("foo", "bar", "baz")),
    )

    assert_that(object_shape, equal_to(expected_object_shape))
