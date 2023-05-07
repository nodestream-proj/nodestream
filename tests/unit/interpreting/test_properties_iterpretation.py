from hamcrest import assert_that, equal_to, has_entries

from nodestream.interpreting.properties_interpretation import (
    PropertiesInterpretation,
)
from nodestream.model import (
    GraphObjectShape,
    GraphObjectType,
    PropertyMetadataSet,
    UnknownTypeMarker,
)

from ..stubs import StubbedValueProvider


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


def test_gather_object_shapes():
    subject = PropertiesInterpretation(properties={"a": "b"})
    shape = next(subject.gather_object_shapes())
    expected_shape = GraphObjectShape(
        graph_object_type=GraphObjectType.NODE,
        object_type=UnknownTypeMarker.source_node(),
        properties=PropertyMetadataSet.from_names(("a",)),
    )
    assert_that(shape, equal_to(expected_shape))
