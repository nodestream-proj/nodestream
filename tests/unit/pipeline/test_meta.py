from hamcrest import assert_that, equal_to

from nodestream.pipeline.meta import (
    UNKNOWN_PIPELINE_NAME,
    UNKNOWN_PIPELINE_SCOPE,
    get_context,
    start_context,
)


def test_get_pipeline_name_unset():
    assert_that(get_context().name, equal_to(UNKNOWN_PIPELINE_NAME))
    assert_that(get_context().scope, equal_to(UNKNOWN_PIPELINE_SCOPE))


def test_get_pipeline_name_and_scope_set():
    with start_context("test", "test_scope"):
        assert_that(get_context().name, equal_to("test"))
        assert_that(get_context().scope, equal_to("test_scope"))
    assert_that(get_context().name, equal_to(UNKNOWN_PIPELINE_NAME))
    assert_that(get_context().scope, equal_to(UNKNOWN_PIPELINE_SCOPE))
