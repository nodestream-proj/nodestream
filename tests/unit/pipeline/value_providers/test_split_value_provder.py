import pytest

from nodestream.pipeline.value_providers import SplitValueProvider
from nodestream.pipeline.value_providers.value_provider import ValueProviderException

from ...stubs import StubbedValueProvider, ErrorValueProvider


def test_split(blank_context):
    subject = SplitValueProvider(delimiter=",", data=StubbedValueProvider(["a,b,c,d"]))
    assert list(subject.many_values(blank_context)) == ["a", "b", "c", "d"]


def test_split_single(blank_context):
    subject = SplitValueProvider(delimiter=",", data=StubbedValueProvider(["a,b,c,d"]))
    assert subject.single_value(blank_context) == "a"


def test_split_not_string(blank_context):
    with pytest.raises(ValueProviderException):
        subject = SplitValueProvider(delimiter=",", data=StubbedValueProvider([1]))
        subject.single_value(blank_context)


def test_split_no_delimiter(blank_context):
    subject = SplitValueProvider(delimiter=",", data=StubbedValueProvider(["abcd"]))
    assert list(subject.many_values(blank_context)) == ["abcd"]


def test_split_empty(blank_context):
    subject = SplitValueProvider(delimiter=",", data=StubbedValueProvider([""]))
    assert list(subject.many_values(blank_context)) == [""]


def test_single_value_error(blank_context_with_document):
    some_text_from_document = blank_context_with_document.document["team"]["name"]
    subject = SplitValueProvider(delimiter=",", data=ErrorValueProvider())

    with pytest.raises(ValueProviderException) as e_info:
        subject.single_value(blank_context_with_document)
    error_message = str(e_info.value)

    assert subject.delimiter in error_message
    assert str(subject.data) in error_message
    assert some_text_from_document in error_message


def test_multiple_values_error(blank_context_with_document):
    some_text_from_document = blank_context_with_document.document["team"]["name"]
    subject = SplitValueProvider(delimiter=",", data=ErrorValueProvider())

    with pytest.raises(ValueProviderException) as e_info:
        iterable = subject.many_values(blank_context_with_document)
        list(iterable)
    error_message = str(e_info.value)

    assert subject.delimiter in error_message
    assert str(subject.data) in error_message
    assert some_text_from_document in error_message
