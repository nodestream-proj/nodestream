from nodestream.pipeline.meta import (
    UNKNOWN_PIPELINE_NAME,
    get_pipeline_name,
    set_pipeline_name,
)


def test_get_pipeline_name_unset():
    assert get_pipeline_name() == UNKNOWN_PIPELINE_NAME


def test_get_pipeline_name_set():
    with set_pipeline_name("test"):
        assert get_pipeline_name() == "test"
    assert get_pipeline_name() == UNKNOWN_PIPELINE_NAME
