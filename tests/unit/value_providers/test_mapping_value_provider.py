import pytest
from hamcrest import assert_that, none, equal_to

from nodestream.value_providers import MappingValueProvider

from ..stubs import StubbedValueProvider

MAPPINGS = {"MAP_A": {"static": "hello", "dynamic": StubbedValueProvider(["world"])}}


@pytest.fixture
def blank_context_with_mapping(blank_context):
    blank_context.mappings = MAPPINGS
    return blank_context


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
