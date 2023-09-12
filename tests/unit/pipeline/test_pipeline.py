import pytest

from nodestream.pipeline.pipeline import Pipeline, empty_async_generator
from nodestream.pipeline.step import PassStep


@pytest.fixture
def pipeline(mocker):
    s1, s2 = PassStep(), PassStep()
    s1.handle_async_record_stream = mocker.Mock(return_value=empty_async_generator())
    s2.handle_async_record_stream = mocker.Mock(return_value=empty_async_generator())
    s1.finish, s2.finish = mocker.AsyncMock(), mocker.AsyncMock()
    return Pipeline([s1, s2], 10)


@pytest.mark.asyncio
async def test_pipeline_run(pipeline):
    await pipeline.run()
    for step in pipeline.steps:
        step.handle_async_record_stream.assert_called_once()
        step.finish.assert_awaited_once()
