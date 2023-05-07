import jq

from hamcrest import assert_that, equal_to, none, has_length

from nodestream.value_providers import JqValueProvider


def test_single_value_present(blank_context_with_document):
    subject = JqValueProvider(jq.compile(".team.name"))
    assert_that(
        subject.single_value(blank_context_with_document), equal_to("nodestream")
    )


def test_single_value_missing(blank_context_with_document):
    subject = JqValueProvider(jq.compile(".team.description"))
    assert_that(subject.single_value(blank_context_with_document), none())


def test_single_value_is_list(blank_context_with_document):
    subject = JqValueProvider(jq.compile(".project.tags"))
    result = subject.single_value(blank_context_with_document)
    assert_that(result, equal_to("graphdb"))


def test_multiple_values_missing(blank_context_with_document):
    subject = JqValueProvider(jq.compile(".project.labels"))
    assert_that(list(subject.many_values(blank_context_with_document)), has_length(0))


def test_multiple_values_returns_one_value(blank_context_with_document):
    subject = JqValueProvider(jq.compile(".team.name"))
    result = subject.many_values(blank_context_with_document)
    assert_that(result, has_length(1))
    assert_that(result[0], equal_to("nodestream"))


def test_multiple_values_hit(blank_context_with_document):
    subject = JqValueProvider(jq.compile(".project.tags"))
    result = subject.many_values(blank_context_with_document)
    assert_that(result, equal_to(["graphdb", "python"]))
