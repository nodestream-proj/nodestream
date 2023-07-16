from hamcrest import assert_that, equal_to

from nodestream.pipeline.meta import (
    STAT_INCREMENTED,
    UNKNOWN_PIPELINE_NAME,
    get_context,
    listen,
    start_context,
)


def test_get_pipeline_name_unset():
    assert_that(get_context().name, equal_to(UNKNOWN_PIPELINE_NAME))


def test_get_pipeline_name_set():
    with start_context("test"):
        assert_that(get_context().name, equal_to("test"))
    assert_that(get_context().name, equal_to(UNKNOWN_PIPELINE_NAME))


def test_callback_gets_event(mocker):
    listen(STAT_INCREMENTED)(command := mocker.Mock())
    get_context().increment_stat("stat", 1000)
    command.assert_called_once_with("stat", 1000)


def test_callback_ignored_when_wrong_event(mocker):
    listen("SOME_EVENT")(command := mocker.Mock())
    get_context().increment_stat("stat", 1000)
    command.assert_not_called()
