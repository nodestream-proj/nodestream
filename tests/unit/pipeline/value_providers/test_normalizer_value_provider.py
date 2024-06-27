import pytest

from nodestream.pipeline.value_providers import NormalizerValueProvider
from nodestream.pipeline.value_providers.value_provider import ValueProviderException
from nodestream.subclass_registry import MissingFromRegistryError

from ...stubs import ErrorValueProvider, StubbedValueProvider


def test_single_value(blank_context):
    subject = NormalizerValueProvider(
        using="lowercase_strings", data=StubbedValueProvider(["ABC"])
    )
    assert subject.single_value(blank_context) == "abc"


def test_many_values(blank_context):
    subject = NormalizerValueProvider(
        using="lowercase_strings", data=StubbedValueProvider(["ABC", "DEF"])
    )
    assert list(subject.many_values(blank_context)) == ["abc", "def"]


def test_invalid_normalizer(blank_context):
    with pytest.raises(MissingFromRegistryError):
        NormalizerValueProvider(
            using="not_a_normalizer", data=StubbedValueProvider(["ABC"])
        )


def test_empty_results_single(blank_context):
    subject = NormalizerValueProvider(
        using="lowercase_strings", data=StubbedValueProvider([])
    )
    assert subject.single_value(blank_context) is None


def test_empty_results_many(blank_context):
    subject = NormalizerValueProvider(
        using="lowercase_strings", data=StubbedValueProvider([])
    )
    assert list(subject.many_values(blank_context)) == []


def test_single_value_error(blank_context_with_document):
    some_text_from_document = blank_context_with_document.document["team"]["name"]
    using = "lowercase_strings"
    subject = NormalizerValueProvider(using=using, data=ErrorValueProvider())

    with pytest.raises(ValueProviderException) as e_info:
        subject.single_value(blank_context_with_document)
    error_message = str(e_info.value)

    assert using in error_message
    assert some_text_from_document in error_message


def test_multiple_values_error(blank_context_with_document):
    some_text_from_document = blank_context_with_document.document["team"]["name"]
    using = "lowercase_strings"
    subject = NormalizerValueProvider(using=using, data=ErrorValueProvider())

    with pytest.raises(ValueProviderException) as e_info:
        iterable = subject.many_values(blank_context_with_document)
        list(iterable)
    error_message = str(e_info.value)

    assert using in error_message
    assert some_text_from_document in error_message
