import pytest
from hamcrest import assert_that, equal_to, equal_to_ignoring_whitespace
from yaml import safe_dump

from nodestream.model import PropertySet
from nodestream.pipeline.value_providers import VariableValueProvider

from ...stubs import StubbedValueProvider

MAPPINGS = {"MAP_A": {"static": "hello", "dynamic": StubbedValueProvider(["world"])}}


@pytest.fixture
def blank_context_with_variables(blank_context):
    blank_context.variables = PropertySet({"a": "value", "b": ["a", "list"]})
    return blank_context


def test_key_miss_single(blank_context_with_variables):
    subject = VariableValueProvider("a")
    assert_that(subject.single_value(blank_context_with_variables), equal_to("value"))


def test_hey_hit_single(blank_context_with_variables):
    subject = VariableValueProvider("b")
    assert_that(
        subject.single_value(blank_context_with_variables), equal_to(["a", "list"])
    )


def test_key_miss_many(blank_context_with_variables):
    subject = VariableValueProvider("c")
    assert_that(subject.many_values(blank_context_with_variables), equal_to([]))


def test_hey_hit_many(blank_context_with_variables):
    subject = VariableValueProvider("a")
    assert_that(subject.many_values(blank_context_with_variables), equal_to(["value"]))


def test_hey_hit_many_from_list(blank_context_with_variables):
    subject = VariableValueProvider("b")
    assert_that(
        subject.many_values(blank_context_with_variables), equal_to(["a", "list"])
    )


def test_variables_dump():
    subject = VariableValueProvider("a")
    assert_that(safe_dump(subject), equal_to_ignoring_whitespace("!variable 'a'"))
