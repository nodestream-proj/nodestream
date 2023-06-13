from hamcrest import assert_that, equal_to

from nodestream.pipeline.meta import UNKNOWN_PIPELINE_NAME, get_context, start_context


def test_get_pipeline_name_unset():
    assert_that(get_context().name, equal_to(UNKNOWN_PIPELINE_NAME))


def test_get_pipeline_name_set():
    with start_context("test"):
        assert_that(get_context().name, equal_to("test"))
    assert_that(get_context().name, equal_to(UNKNOWN_PIPELINE_NAME))
