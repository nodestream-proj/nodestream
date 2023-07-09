from hamcrest import assert_that, equal_to

from nodestream.pipeline.value_providers import StaticValueProvider


def test_single_value(blank_context):
    expected_value = "foo"
    subject = StaticValueProvider(expected_value)
    assert_that(subject.single_value(blank_context), equal_to(expected_value))


def test_many_values_not_list(blank_context):
    expected_value = "foo"
    subject = StaticValueProvider([expected_value])
    assert_that(subject.many_values(blank_context), equal_to([expected_value]))


def test_many_values_list(blank_context):
    expected = [1, 2, 3]
    subject = StaticValueProvider(expected)
    assert_that(subject.many_values(blank_context), equal_to(expected))
