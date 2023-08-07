import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline.value_providers import RegexValueProvider

from ...stubs import StubbedValueProvider


@pytest.fixture
def subject_with_named_groups():
    return RegexValueProvider(
        regex="(?P<name>[a-z]+) (?P<age>[0-9]+)",
        data=StubbedValueProvider(["john 42", "jane 32"]),
        group="name",
    )


def test_default_group(blank_context, subject_with_named_groups):
    subject_with_named_groups.group = 0
    assert_that(
        subject_with_named_groups.single_value(blank_context), equal_to("john 42")
    )


def test_single_value_matching_regex(blank_context, subject_with_named_groups):
    assert_that(subject_with_named_groups.single_value(blank_context), equal_to("john"))


def test_many_values_matching_regex(blank_context, subject_with_named_groups):
    result = list(subject_with_named_groups.many_values(blank_context))
    assert_that(result, equal_to(["john", "jane"]))


def test_single_value_not_matching_regex(blank_context, subject_with_named_groups):
    subject_with_named_groups.data = StubbedValueProvider(["john", "jane"])
    assert_that(subject_with_named_groups.single_value(blank_context), equal_to(None))


def test_many_values_not_matching_regex(blank_context, subject_with_named_groups):
    subject_with_named_groups.data = StubbedValueProvider(["john", "jane"])
    result = list(subject_with_named_groups.many_values(blank_context))
    assert_that(result, equal_to([None, None]))


def test_numeric_group(blank_context, subject_with_named_groups):
    subject_with_named_groups.group = 2
    assert_that(subject_with_named_groups.single_value(blank_context), equal_to("42"))
