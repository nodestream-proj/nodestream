from nodestream.pipeline.meta import (
    get_pipeline_name,
    set_pipeline_name,
    UKNOWN_PIPELINE_NAME,
)


def test_get_pipeline_name_unset():
    assert get_pipeline_name() == UKNOWN_PIPELINE_NAME


def test_get_pipeline_name_set():
    with set_pipeline_name("test"):
        assert get_pipeline_name() == "test"
    assert get_pipeline_name() == UKNOWN_PIPELINE_NAME
