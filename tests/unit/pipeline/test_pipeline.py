import pytest

from nodestream.pipeline import Pipeline


@pytest.fixture
def pipeline(mocker):
    steps = [mocker.Mock(), mocker.Mock()]
    return Pipeline(steps)


def test_pipeline_run(pipeline):
    pipeline.run()
    for step in pipeline.steps:
        step.handle_async_record_stream.assert_called_once()
