from hamcrest import assert_that, equal_to

from nodestream.value_providers import StringFormattingValueProvider

from ..stubs import StubbedValueProvider


def test_string_format(blank_context):
    subject = StringFormattingValueProvider(
        fmt="{a}{b}", a=StubbedValueProvider(["a"]), b=StubbedValueProvider(["b"])
    )
    assert_that(subject.single_value(blank_context), equal_to("ab"))


def test_string_format_many(blank_context):
    subject = StringFormattingValueProvider(
        fmt="{a}{b}", a=StubbedValueProvider(["a"]), b=StubbedValueProvider(["b"])
    )
    assert_that(subject.many_values(blank_context), equal_to(["ab"]))
