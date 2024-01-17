from hamcrest import assert_that, has_entries

from nodestream.interpreting.interpretations import (
    PropertiesInterpretation,
    SourceNodeInterpretation,
)

from ...stubs import StubbedValueProvider
from .matchers import has_node_properties


def test_properties_interpretaton_applies_static_properties(blank_context):
    expected_properties = {"first_name": "Zach", "last_name": "Probst"}
    subject = PropertiesInterpretation(properties=expected_properties)
    subject.interpret(blank_context)
    actual_properties = blank_context.desired_ingest.source.properties
    assert_that(actual_properties, has_entries(expected_properties))


def test_properties_interpretation_applies_dynamic_properties(blank_context):
    first_name = "Zach"
    expected_properties = {"first_name": first_name, "last_name": "Probst"}
    dynamic_first_name = StubbedValueProvider(values=[first_name])
    subject = PropertiesInterpretation(
        properties={"first_name": dynamic_first_name, "last_name": "Probst"},
    )
    subject.interpret(blank_context)
    actual_properties = blank_context.desired_ingest.source.properties
    assert_that(actual_properties, has_entries(expected_properties))


def test_expand_schmea_adds_properties(schema_coordinator):
    subject = PropertiesInterpretation(properties={"a": "b"})
    subject.expand_schema(schema_coordinator)
    assert_that(
        schema_coordinator,
        has_node_properties(SourceNodeInterpretation.SOURCE_NODE_TYPE_ALIAS, ("a",)),
    )
