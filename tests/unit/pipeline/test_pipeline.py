import pytest

from nodestream.pipeline.pipeline import (
    PipelineOutput,
    PipelineProgressReporter,
    Step,
    StepContext,
    StepExecutor,
    StepInput,
    StepOutput,
)


@pytest.fixture
def step_executor(mocker):
    step = mocker.Mock(Step)
    input = mocker.Mock(StepInput)
    output = mocker.Mock(StepOutput)
    context = mocker.Mock(StepContext)

    return StepExecutor(step, input, output, context)


@pytest.mark.asyncio
async def test_start_step(step_executor):
    await step_executor.start_step()
    step_executor.step.start.assert_called_once_with(step_executor.context)


@pytest.mark.asyncio
async def test_start_step_error(step_executor, mocker):
    step_executor.step.start.side_effect = Exception("Boom")
    await step_executor.start_step()
    step_executor.context.report_error.assert_called_once_with(
        "Error starting step", mocker.ANY
    )


@pytest.mark.asyncio
async def test_stop_step(step_executor):
    await step_executor.stop_step()
    step_executor.step.finish.assert_called_once_with(step_executor.context)


@pytest.mark.asyncio
async def test_stop_step_error(step_executor, mocker):
    step_executor.step.finish.side_effect = Exception("Boom")
    await step_executor.stop_step()
    step_executor.context.report_error.assert_called_once_with(
        "Error stopping step", mocker.ANY
    )


@pytest.mark.asyncio
async def test_emit_record(step_executor, mocker):
    record = mocker.Mock()
    step_executor.output.put.return_value = True
    await step_executor.emit_record(record)
    step_executor.output.put.assert_called_once_with(record)


@pytest.mark.asyncio
async def test_emit_record_full(step_executor, mocker):
    record = mocker.Mock()
    step_executor.output.put.return_value = False
    await step_executor.emit_record(record)
    step_executor.output.put.assert_called_once_with(record)
    step_executor.context.debug.assert_called_once_with(
        "Downstream is not accepting more records. Gracefully stopping."
    )


@pytest.mark.asyncio
async def test_drive_step(step_executor, mocker):
    record = mocker.Mock()
    step = Step()
    step_executor.step = step
    step_executor.input.get = mocker.AsyncMock(side_effect=[record, None])
    await step_executor.drive_step()
    step_executor.output.put.assert_called_once_with(record)
    step_executor.context.debug.assert_called_once_with("Step finished emitting")


@pytest.mark.asyncio
async def test_drive_step_error(step_executor, mocker):
    step_executor.input.get.side_effect = Exception("Boom")
    await step_executor.drive_step()
    step_executor.context.report_error.assert_called_once_with(
        "Error running step",
        mocker.ANY,
        fatal=True,
    )


@pytest.mark.asyncio
async def test_drive_step_cannot_continue(step_executor, mocker):
    record = mocker.Mock()
    step = Step()
    step_executor.step = step
    step_executor.input.get = mocker.AsyncMock(side_effect=[record, None])
    step_executor.output.put.return_value = False
    await step_executor.drive_step()
    step_executor.output.put.assert_called_once_with(record)


@pytest.mark.asyncio
async def test_drive_step_cannot_continue_in_emit_outstanding_records(
    step_executor, mocker
):
    record = mocker.Mock()

    async def emit_outstanding_records():
        yield record

    step_executor.input.get = mocker.AsyncMock(side_effect=[None])
    step_executor.step.emit_outstanding_records.return_value = (
        emit_outstanding_records()
    )
    step_executor.output.put.return_value = False
    await step_executor.drive_step()
    step_executor.output.put.assert_called_once_with(record)


@pytest.mark.asyncio
async def test_pipeline_output_call_handling_errors(mocker):
    def on_start_callback():
        raise Exception("Boom")

    output = PipelineOutput(
        mocker.Mock(StepInput),
        PipelineProgressReporter(
            on_start_callback=on_start_callback,
            logger=mocker.Mock(),
        ),
    )

    output.call_handling_errors(output.reporter.on_start_callback)
    output.reporter.logger.exception.assert_called_once()


@pytest.mark.asyncio
async def test_pipeline_output_on_finish_callback_exceptions_not_swallowed(
    mocker,
):
    """Test that on_finish_callback exceptions are not swallowed.

    This test covers the case described in the diff comment:
    'For cases where the reporter _wants_ to have the exception thrown
    (e.g to have a status code in the CLI) we need to make sure we call
    on_finish_callback without swallowing exceptions (because thats the
    point).'
    """

    def on_finish_callback(metrics):
        raise ValueError("CLI status code exception")

    input_mock = mocker.Mock(StepInput)
    # No records to process
    input_mock.get = mocker.AsyncMock(return_value=None)

    output = PipelineOutput(
        input_mock,
        PipelineProgressReporter(
            on_finish_callback=on_finish_callback,
            logger=mocker.Mock(),
        ),
    )

    # The exception should propagate up and not be caught
    with pytest.raises(ValueError, match="CLI status code exception"):
        await output.run()


@pytest.mark.asyncio
async def test_pipeline_on_start_callback_called_before_step_operations(
    mocker,
):
    """Test that on_start_callback is called before any step operations begin.

    This test covers the case described in the diff comment:
    'Here lies a footgun. DO NOT MOVE the `create_task` call after the
    running_steps generator. It will break the pipeline because we need
    to ensure the on_start_callback is called before any operations
    occur in the actual steps in the pipeline.'
    """
    from nodestream.pipeline.pipeline import Pipeline
    from nodestream.pipeline.object_storage import ObjectStore

    # Track the order of operations
    call_order = []

    def on_start_callback():
        call_order.append("on_start_callback")

    def on_finish_callback(metrics):
        call_order.append("on_finish_callback")

    # Create a mock step that records when it starts
    mock_step = mocker.Mock(Step)
    mock_step.__class__.__name__ = "MockStep"

    async def mock_start(context):
        call_order.append("step_start")

    async def mock_finish(context):
        call_order.append("step_finish")

    async def mock_process_record(record, context):
        call_order.append("step_process")
        yield record

    async def mock_emit_outstanding_records(context):
        call_order.append("step_emit_outstanding")
        for record in ():
            yield record

    mock_step.start = mock_start
    mock_step.finish = mock_finish
    mock_step.process_record = mock_process_record
    mock_step.emit_outstanding_records = mock_emit_outstanding_records

    # Create pipeline with the mock step
    object_store = mocker.Mock(spec=ObjectStore)
    object_store.namespaced.return_value = object_store

    pipeline = Pipeline((mock_step,), 10, object_store)

    reporter = PipelineProgressReporter(
        on_start_callback=on_start_callback,
        on_finish_callback=on_finish_callback,
        logger=mocker.Mock(),
    )

    await pipeline.run(reporter)

    # Verify that on_start_callback was called before any step operations
    assert call_order[0] == "on_start_callback"
    assert "step_start" in call_order
    assert call_order.index("on_start_callback") < call_order.index("step_start")
