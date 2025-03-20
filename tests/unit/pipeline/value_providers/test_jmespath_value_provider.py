from collections.abc import Generator

import pytest

from nodestream.model import DesiredIngestion
from nodestream.pipeline.value_providers import JmespathValueProvider, ProviderContext
from nodestream.pipeline.value_providers.value_provider import ValueProviderException


def test_single_value_present(blank_context_with_document):
    subject = JmespathValueProvider.from_string_expression("team.name")
    assert subject.single_value(blank_context_with_document) == "nodestream"


def test_single_value_present_complicated(blank_context_with_document):
    subject = JmespathValueProvider.from_string_expression("*.name")
    assert subject.single_value(blank_context_with_document) == [
        "nodestream",
        "project_name",
    ]


def test_single_value_present_complicated_always_arr(blank_context_with_document):
    context = ProviderContext(
        {"toplevel": {"level2": [{"name": "first"}, {"name": "second"}]}},
        DesiredIngestion(),
    )
    subject = JmespathValueProvider.from_string_expression("toplevel.level2[].name")
    assert subject.single_value(context) == [
        "first",
        "second",
    ]


def test_many_values_present_complicated_always_arr(blank_context_with_document):
    context = ProviderContext(
        {"toplevel": {"level2": [{"name": "first"}, {"name": "second"}]}},
        DesiredIngestion(),
    )
    subject = JmespathValueProvider.from_string_expression("toplevel.level2[].name")
    result = subject.many_values(context)

    assert isinstance(result, Generator)
    assert next(result) == "first"
    assert next(result) == "second"
    with pytest.raises(StopIteration):
        next(result)


def test_single_value_missing(blank_context_with_document):
    subject = JmespathValueProvider.from_string_expression("team.description")
    assert subject.single_value(blank_context_with_document) is None


def test_single_value_is_list(blank_context_with_document):
    subject = JmespathValueProvider.from_string_expression("project.tags")
    result = subject.single_value(blank_context_with_document)
    assert result == ["graphdb", "python"]


def test_multiple_values_missing(blank_context_with_document):
    subject = JmespathValueProvider.from_string_expression("team.description")
    result = list(subject.many_values(blank_context_with_document))
    assert result == []


def test_many_values_dict_return():
    context = ProviderContext({"a": {"b": {"name": "example"}}}, DesiredIngestion())
    subject = JmespathValueProvider.from_string_expression("a.b")
    result = list(subject.many_values(context))
    assert result == [{"name": "example"}]


def test_single_value_single_level_array():
    context = ProviderContext(
        {"toplevel": ["test1", "test2", "test3"]}, DesiredIngestion()
    )
    subject = JmespathValueProvider.from_string_expression("toplevel")
    result = list(subject.single_value(context))
    assert result == ["test1", "test2", "test3"]


def test_many_values_single_level_array():
    context = ProviderContext(
        {"toplevel": ["test1", "test2", "test3"]}, DesiredIngestion()
    )
    subject = JmespathValueProvider.from_string_expression("toplevel")
    result = list(subject.single_value(context))
    assert result == ["test1", "test2", "test3"]


def test_multiple_values_returns_one_value(blank_context_with_document):
    subject = JmespathValueProvider.from_string_expression("team.name")
    result = list(subject.many_values(blank_context_with_document))
    assert result == ["nodestream"]


def test_multiple_values_hit(blank_context_with_document):
    subject = JmespathValueProvider.from_string_expression("project.tags")
    result = list(subject.many_values(blank_context_with_document))
    assert result == ["graphdb", "python"]


def test_single_value_error(blank_context_with_document):
    some_text_from_document = blank_context_with_document.document["team"]["name"]
    # this will error because team2 does not exist causing the join to throw an error
    expression_with_error = "join('/', [team.name || '', team2.name])"
    subject = JmespathValueProvider.from_string_expression(expression_with_error)

    with pytest.raises(ValueProviderException) as e_info:
        subject.single_value(blank_context_with_document)
    error_message = str(e_info.value)

    assert expression_with_error in error_message
    assert some_text_from_document in error_message


def test_multiple_values_error(blank_context_with_document):
    # this will error because team2 does not exist causing the join to throw an error
    expression_with_error = "join('/', [team.name || '', team2.name])"
    subject = JmespathValueProvider.from_string_expression(expression_with_error)

    with pytest.raises(ValueProviderException) as e_info:
        list(subject.many_values(blank_context_with_document))

    error_message = str(e_info.value)

    assert expression_with_error in error_message
