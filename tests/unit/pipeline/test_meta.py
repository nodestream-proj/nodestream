from hamcrest import assert_that, equal_to

from nodestream.pipeline.meta import (
    UNKNOWN_PIPELINE_NAME,
    get_pipeline_name,
    set_pipeline_name,
)


def test_get_pipeline_name_unset():
    assert_that(get_pipeline_name(), equal_to(UNKNOWN_PIPELINE_NAME))


def test_get_pipeline_name_set():
    with set_pipeline_name("test"):
        assert_that(get_pipeline_name(), equal_to("test"))
    assert_that(get_pipeline_name(), equal_to(UNKNOWN_PIPELINE_NAME))
