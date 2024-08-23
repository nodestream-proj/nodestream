import jmespath
import pytest
from hamcrest import assert_that, equal_to, has_length, none

from nodestream.pipeline.value_providers import JmespathValueProvider
from nodestream.pipeline.value_providers.value_provider import ValueProviderException


def test_single_value_present(blank_context_with_document):
    subject = JmespathValueProvider(jmespath.compile("team.name"))
    assert_that(
        subject.single_value(blank_context_with_document), equal_to("nodestream")
    )


def test_single_value_missing(blank_context_with_document):
    subject = JmespathValueProvider(jmespath.compile("team.description"))
    assert_that(subject.single_value(blank_context_with_document), none())


def test_single_value_is_list(blank_context_with_document):
    subject = JmespathValueProvider(jmespath.compile("project.tags"))
    result = subject.single_value(blank_context_with_document)
    assert_that(result, equal_to("graphdb"))


def test_multiple_values_missing(blank_context_with_document):
    subject = JmespathValueProvider(jmespath.compile("project.labels"))
    assert_that(list(subject.many_values(blank_context_with_document)), has_length(0))


def test_multiple_values_returns_one_value(blank_context_with_document):
    subject = JmespathValueProvider(jmespath.compile("team.name"))
    result = list(subject.many_values(blank_context_with_document))
    assert_that(result, has_length(1))
    assert_that(result[0], equal_to("nodestream"))


def test_multiple_values_hit(blank_context_with_document):
    subject = JmespathValueProvider(jmespath.compile("project.tags"))
    result = subject.many_values(blank_context_with_document)
    assert_that(list(result), equal_to(["graphdb", "python"]))


def test_single_value_error(blank_context_with_document):
    some_text_from_document = blank_context_with_document.document["team"]["name"]
    # this will error because team2 does not exist causing the join to throw an error
    expression_with_error = "join('/', [team.name || '', team2.name])"
    subject = JmespathValueProvider(jmespath.compile(expression_with_error))

    with pytest.raises(ValueProviderException) as e_info:
        subject.single_value(blank_context_with_document)
    error_message = str(e_info.value)

    assert expression_with_error in error_message
    assert some_text_from_document in error_message


def test_multiple_values_error(blank_context_with_document):
    # this will error because team2 does not exist causing the join to throw an error
    expression_with_error = "join('/', [team.name || '', team2.name])"
    subject = JmespathValueProvider(jmespath.compile(expression_with_error))

    with pytest.raises(Exception) as e_info:
        generator = subject.many_values(blank_context_with_document)
        list(generator)
    error_message = str(e_info.value)

    assert expression_with_error in error_message
