import pytest

from nodestream.pipeline import PipelineProgressReporter
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


@pytest.mark.asyncio
async def test_pipeline_run_with_error_calls_finish(pipeline, mocker):
    pipeline.steps[0].handle_async_record_stream = mocker.Mock(
        side_effect=Exception("test")
    )

    await pipeline.run()
    for step in pipeline.steps:
        step.handle_async_record_stream.assert_called_once()
        step.finish.assert_awaited_once()


@pytest.mark.asyncio
async def test_pipeline_run_with_error_on_start(pipeline, mocker):
    await pipeline.run(
        PipelineProgressReporter(
            on_start_callback=mocker.Mock(side_effect=Exception("test"))
        )
    )
    for step in pipeline.steps:
        step.handle_async_record_stream.assert_called_once()
        step.finish.assert_awaited_once()


@pytest.mark.asyncio
async def test_pipeline_run_with_error_on_finish(pipeline, mocker):
    await pipeline.run(
        PipelineProgressReporter(
            on_finish_callback=mocker.Mock(side_effect=Exception("test"))
        )
    )
    for step in pipeline.steps:
        step.handle_async_record_stream.assert_called_once()
        step.finish.assert_awaited_once()
