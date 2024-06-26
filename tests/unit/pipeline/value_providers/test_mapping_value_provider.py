import pytest
from hamcrest import assert_that, equal_to, none
from yaml import safe_dump

from nodestream.pipeline.value_providers import MappingValueProvider
from nodestream.pipeline.value_providers.value_provider import ValueProviderException

from ...stubs import StubbedValueProvider, ErrorValueProvider

MAPPINGS = {
    "MAP_A": {
        "static": "hello",
        "dynamic": StubbedValueProvider(["world"]),
        "error": ErrorValueProvider()
    }
}


@pytest.fixture
def blank_context_with_mapping(blank_context_with_document):
    blank_context_with_document.mappings = MAPPINGS
    return blank_context_with_document


def test_map_miss(blank_context_with_mapping):
    subject = MappingValueProvider("MAP_B", "static")
    assert_that(subject.single_value(blank_context_with_mapping), none())


def test_map_hit_key_miss(blank_context_with_mapping):
    subject = MappingValueProvider("MAP_A", "other")
    assert_that(subject.single_value(blank_context_with_mapping), none())


def test_map_hit_key_hit_static_value(blank_context_with_mapping):
    subject = MappingValueProvider("MAP_A", "static")
    assert_that(subject.single_value(blank_context_with_mapping), equal_to("hello"))


def test_map_hit_key_hit_dynamic_value(blank_context_with_mapping):
    subject = MappingValueProvider("MAP_A", "dynamic")
    assert_that(subject.single_value(blank_context_with_mapping), equal_to("world"))


def test_many_values_hit(blank_context_with_mapping):
    subject = MappingValueProvider("MAP_A", "dynamic")
    assert_that(
        list(subject.many_values(blank_context_with_mapping)), equal_to(["world"])
    )


def test_many_values_miss(blank_context_with_mapping):
    subject = MappingValueProvider("MAP_A", "other")
    assert_that(subject.many_values(blank_context_with_mapping), equal_to([]))


def test_mapping_dump():
    subject = MappingValueProvider("MAP_A", "static")
    assert_that(
        safe_dump(subject),
        equal_to("!mapping\nkey: !static 'static'\nmapping_name: MAP_A\n"),
    )


def test_single_value_error(blank_context_with_mapping):
    mapping_name = "MAP_A"
    key = "error"
    some_text_from_document = blank_context_with_mapping.document["team"]["name"]
    subject = MappingValueProvider(mapping_name, key)

    with pytest.raises(ValueProviderException) as e_info:
        subject.single_value(blank_context_with_mapping)
    error_message = str(e_info.value)

    assert mapping_name in error_message
    assert key in error_message
    assert some_text_from_document in error_message


def test_multiple_values_error(blank_context_with_mapping):
    mapping_name = "MAP_A"
    key = "error"
    some_text_from_document = blank_context_with_mapping.document["team"]["name"]
    subject = MappingValueProvider(mapping_name, key)

    with pytest.raises(ValueProviderException) as e_info:
        iterable = subject.many_values(blank_context_with_mapping)
        next(iterable)
    error_message = str(e_info.value)

    assert mapping_name in error_message
    assert key in error_message
    assert some_text_from_document in error_message
