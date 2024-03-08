from hamcrest import assert_that, equal_to

from nodestream.model import PropertySet
from nodestream.interpreting.interpretations.property_mapping import PropertyMapping


from ...stubs import StubbedValueProvider


def test_property_mapping_from_dict(blank_context):
    expected_properties = {"first_name": "zach", "last_name": "probst"}
    subject = PropertyMapping.from_file_data(expected_properties)
    target = PropertySet()
    subject.apply_to(blank_context, target, {})
    assert_that(target, equal_to(expected_properties))


def test_property_mapping_from_dict_known_keys(blank_context):
    expected_properties = {"first_name": "zach", "last_name": "probst"}
    subject = PropertyMapping.from_file_data(expected_properties)
    result = list(subject)
    assert_that(result, equal_to(["first_name", "last_name"]))


def test_property_mapping_from_value_provider(blank_context):
    expected_properties = {"first_name": "zach", "last_name": "probst"}
    value_provider = StubbedValueProvider(values=[expected_properties])
    subject = PropertyMapping.from_file_data(value_provider)
    target = PropertySet()
    subject.apply_to(blank_context, target, {})
    assert_that(target, equal_to(expected_properties))


def test_property_mapping_from_value_provider_known_keys(blank_context):
    expected_properties = {"first_name": "zach", "last_name": "probst"}
    value_provider = StubbedValueProvider(values=[expected_properties])
    subject = PropertyMapping.from_file_data(value_provider)
    result = list(subject)
    assert_that(result, equal_to([]))
