from hamcrest import assert_that, equal_to
from yaml import safe_dump

from nodestream.pipeline.value_providers import (
    StaticValueProvider,
    StringFormattingValueProvider,
)

from ...stubs import StubbedValueProvider


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


def test_string_format_dump():
    subject = StringFormattingValueProvider(
        fmt=StaticValueProvider("{a}"),
        a=StaticValueProvider("a"),
        b=StaticValueProvider("b"),
        c=StaticValueProvider("c"),
    )
    assert_that(
        safe_dump(subject, sort_keys=True),
        equal_to(
            "!format\na: !static 'a'\nb: !static 'b'\nc: !static 'c'\nfmt: !static '{a}'\n"
        ),
    )


def test_string_format_empty(blank_context):
    subject = StringFormattingValueProvider(
        fmt="{a}{b}", a=StaticValueProvider("a"), b=StaticValueProvider(None)
    )
    assert_that(
        subject.single_value(blank_context), equal_to(None)
    )