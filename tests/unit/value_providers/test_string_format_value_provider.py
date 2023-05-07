from nodestream.value_providers import StringFormattingValueProvider

from hamcrest import assert_that, equal_to

from ..stubs import StubbedValueProvider


def test_string_format(blank_context):
    subject = StringFormattingValueProvider(
        fmt="{a}{b}", a=StubbedValueProvider(["a"]), b=StubbedValueProvider(["b"])
    )
    assert_that(subject.single_value(blank_context), "ab")


def test_string_format_many(blank_context):
    subject = StringFormattingValueProvider(
        fmt="{a}{b}", a=StubbedValueProvider(["a"]), b=StubbedValueProvider(["b"])
    )
    assert_that(subject.many_values(blank_context), ["ab"])
