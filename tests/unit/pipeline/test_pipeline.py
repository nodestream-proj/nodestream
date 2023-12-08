import pytest

from nodestream.pipeline import PipelineProgressReporter
from nodestream.pipeline.pipeline import Pipeline, empty_async_generator
from nodestream.pipeline.step import PassStep
from nodestream.pipeline.pipeline import PipelineException, StepException, StepExecutor
from unittest.mock import patch


@pytest.fixture
def pipeline(mocker):
    s1, s2 = PassStep(), PassStep()
    s1.handle_async_record_stream = mocker.Mock(return_value=empty_async_generator())
    s2.handle_async_record_stream = mocker.Mock(return_value=empty_async_generator())
    s1.finish, s2.finish = mocker.AsyncMock(), mocker.AsyncMock()
    return Pipeline([s1, s2], 10)


@pytest.fixture
def step_executor(mocker):
    step = PassStep()
    step.handle_async_record_stream = mocker.Mock(return_value=empty_async_generator())
    step.finish = mocker.AsyncMock()
    return StepExecutor(step=step, upstream=None)


def throw_exception():
    raise Exception("test")


@pytest.mark.asyncio
async def test_pipeline_run(pipeline):
    await pipeline.run()
    for step in pipeline.steps:
        step.handle_async_record_stream.assert_called_once()
        step.finish.assert_awaited_once()


@pytest.mark.asyncio
async def test_pipeline_run_with_error_on_start(pipeline, mocker):
    with pytest.raises(expected_exception=(PipelineException)):
        await pipeline.run(
            PipelineProgressReporter(
                on_start_callback=mocker.Mock(side_effect=Exception("test"))
            )
        )
    for step in pipeline.steps:
        step.handle_async_record_stream.assert_called_once()
        step.finish.assert_awaited_once()


@pytest.mark.asyncio
async def test_pipeline_run_with_error_on_work_body(pipeline):
    for step in pipeline.steps:
        step.handle_async_record_stream.side_effect = Exception("test")

    with pytest.raises(expected_exception=(PipelineException)):
        await pipeline.run()

    for step in pipeline.steps:
        step.finish.assert_called_once()


@pytest.mark.asyncio
async def test_pipeline_run_with_error_on_finish(pipeline, mocker):
    with pytest.raises(expected_exception=(PipelineException)):
        await pipeline.run(
            PipelineProgressReporter(
                on_finish_callback=mocker.Mock(side_effect=Exception("test"))
            )
        )
    for step in pipeline.steps:
        step.handle_async_record_stream.assert_called_once()


@pytest.mark.asyncio
@patch("nodestream.pipeline.pipeline.StepExecutor.start", side_effect=Exception("test"))
async def test_step_executor_throws_start_exception(start_mock, step_executor):
    step = step_executor.step
    with pytest.raises(expected_exception=(StepException)):
        await step_executor.work_loop()
    step.handle_async_record_stream.assert_called_once()
    step.finish.assert_called_once()


@pytest.mark.asyncio
@patch(
    "nodestream.pipeline.pipeline.StepExecutor.work_body", side_effect=Exception("test")
)
async def test_step_executor_throws_work_body_exception(work_body_mock, step_executor):
    step = step_executor.step
    with pytest.raises(expected_exception=(StepException)):
        await step_executor.work_loop()
    step.finish.assert_called_once()


@pytest.mark.asyncio
@patch("nodestream.pipeline.pipeline.StepExecutor.stop", side_effect=Exception("test"))
async def test_step_executor_throws_finish_exception(finish_mock, step_executor):
    step = step_executor.step
    with pytest.raises(expected_exception=(StepException)):
        await step_executor.work_loop()
    step.handle_async_record_stream.assert_called_once()


@pytest.mark.asyncio
@patch("nodestream.pipeline.pipeline.StepExecutor.start", side_effect=Exception("test"))
@patch(
    "nodestream.pipeline.pipeline.StepExecutor.work_body", side_effect=Exception("test")
)
async def test_step_executor_collects_multiple_errors(
    start_mock, work_body_mock, step_executor
):
    with pytest.raises(expected_exception=(StepException)):
        await step_executor.work_loop()
    assert len(step_executor.exceptions) == 2


@pytest.mark.asyncio
@patch("nodestream.pipeline.pipeline.StepExecutor.start", side_effect=Exception("test"))
@patch(
    "nodestream.pipeline.pipeline.StepExecutor.work_body", side_effect=Exception("test")
)
async def test_pipeline_errors_are_kept_in_exception(
    start_mock, work_body_mock, pipeline, mocker
):
    with pytest.raises(expected_exception=(PipelineException)):
        await pipeline.run()
    assert len(pipeline.errors) == 2
    for error in pipeline.errors:
        assert type(error) == StepException
        assert "Exception in Start Process:" in error.exceptions
        assert "Exception in Work Body:" in error.exceptions
