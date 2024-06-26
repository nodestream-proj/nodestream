import pytest
from hamcrest import assert_that, equal_to
from yaml import safe_dump

from nodestream.pipeline.value_providers import (
    StaticValueProvider,
    StringFormattingValueProvider,
)
from nodestream.pipeline.value_providers.value_provider import ValueProviderException

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
    assert_that(subject.single_value(blank_context), equal_to(None))


def test_single_value_error(blank_context_with_document):
    some_text_from_document = blank_context_with_document.document["team"]["name"]
    subject = StringFormattingValueProvider(
        fmt="{a}{b}{c}", a=StubbedValueProvider(["a"]), b=StubbedValueProvider(["b"])
    )

    with pytest.raises(ValueProviderException) as e_info:
        subject.single_value(blank_context_with_document)
    error_message = str(e_info.value)

    assert str(subject.fmt) in error_message
    assert str(subject.subs) in error_message
    assert some_text_from_document in error_message


def test_multiple_values_error(blank_context_with_document):
    some_text_from_document = blank_context_with_document.document["team"]["name"]
    subject = StringFormattingValueProvider(
        fmt="{a}{b}{c}", a=StubbedValueProvider(["a"]), b=StubbedValueProvider(["b"])
    )

    with pytest.raises(ValueProviderException) as e_info:
        iterable = subject.many_values(blank_context_with_document)
        list(iterable)
    error_message = str(e_info.value)

    assert str(subject.fmt) in error_message
    assert str(subject.subs) in error_message
    assert some_text_from_document in error_message
