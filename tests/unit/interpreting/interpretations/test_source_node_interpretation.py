import pytest
from hamcrest import assert_that, equal_to, has_entries

from nodestream.interpreting.interpretations import SourceNodeInterpretation

from ...stubs import StubbedValueProvider
from .matchers import (
    has_no_defined_nodes,
    has_no_defined_relationships,
    has_node_indexes,
    has_node_keys,
    has_node_properties,
    has_no_defined_properties,
)

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


def test_non_static_node_type_skips_node_description(schema_coordinator):
    dynamic_type = StubbedValueProvider(values=["zach"])
    subject = SourceNodeInterpretation(node_type=dynamic_type, key={})
    subject.expand_schema(schema_coordinator)
    assert_that(schema_coordinator, has_no_defined_nodes())


def test_source_node_defines_no_relationships(schema_coordinator):
    subject = SourceNodeInterpretation(node_type="test", key={"first_name": "test"})
    subject.expand_schema(schema_coordinator)
    assert_that(schema_coordinator, has_no_defined_relationships())


def test_introspectable_definition_updates_schema(schema_coordinator):
    subject = SourceNodeInterpretation(
        node_type="test",
        key={"foo": 1, "bar": False},
        additional_indexes=("baz",),
        properties={"baz": False},
    )
    subject.expand_schema(schema_coordinator)
    assert_that(schema_coordinator, has_node_keys("test", ("foo", "bar")))
    assert_that(schema_coordinator, has_node_properties("test", ("baz",)))
    assert_that(schema_coordinator, has_node_indexes("test", ("baz",)))


def test_source_node_interpretation_with_properties_from_value_provider(blank_context):
    subject = SourceNodeInterpretation(
        node_type="Static",
        key={"hello": "world"},
        properties=StubbedValueProvider(values=[{"prop": "value"}]),
    )
    subject.interpret(blank_context)
    assert_that(
        blank_context.desired_ingest.source.properties, has_entries({"prop": "value"})
    )


def test_source_node_interpretation_with_properties_from_value_provider_wrong_type(
    blank_context,
):
    with pytest.raises(ValueError):
        subject = SourceNodeInterpretation(
            node_type="Static",
            key={"hello": "world"},
            properties=StubbedValueProvider(values=["prop"]),
        )
        subject.interpret(blank_context)
