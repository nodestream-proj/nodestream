import pytest

from nodestream.pipeline.value_providers import SplitValueProvider

from ...stubs import StubbedValueProvider


def test_split(blank_context):
    subject = SplitValueProvider(delimiter=",", data=StubbedValueProvider(["a,b,c,d"]))
    assert list(subject.many_values(blank_context)) == ["a", "b", "c", "d"]


def test_split_single(blank_context):
    subject = SplitValueProvider(delimiter=",", data=StubbedValueProvider(["a,b,c,d"]))
    assert subject.single_value(blank_context) == "a"


def test_split_not_string(blank_context):
    with pytest.raises(TypeError):
        subject = SplitValueProvider(delimiter=",", data=StubbedValueProvider([1]))
        subject.single_value(blank_context)


def test_split_no_delimiter(blank_context):
    subject = SplitValueProvider(delimiter=",", data=StubbedValueProvider(["abcd"]))
    assert list(subject.many_values(blank_context)) == ["abcd"]


def test_split_empty(blank_context):
    subject = SplitValueProvider(delimiter=",", data=StubbedValueProvider([""]))
    assert list(subject.many_values(blank_context)) == [""]
