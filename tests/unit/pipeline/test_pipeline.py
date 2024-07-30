import pytest

from nodestream.pipeline.pipeline import (
    StepExecutor,
    StepInput,
    StepOuput,
    StepContext,
    Step,
    PielineOutput,
    PipelineProgressReporter,
)


@pytest.fixture
def step_executor(mocker):
    step = mocker.Mock(Step)
    input = mocker.Mock(StepInput)
    output = mocker.Mock(StepOuput)
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
        f"Error starting step {step_executor.step.__class__.__name__}", mocker.ANY
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
        f"Error stopping step {step_executor.step.__class__.__name__}", mocker.ANY
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
    step_executor.context.debug.assert_called_once_with(
        f"Step {step_executor.step.__class__.__name__} finished emitting"
    )


@pytest.mark.asyncio
async def test_drive_step_error(step_executor, mocker):
    step_executor.input.get.side_effect = Exception("Boom")
    await step_executor.drive_step()
    step_executor.context.report_error.assert_called_once_with(
        f"Error running step {step_executor.step.__class__.__name__}",
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

    output = PielineOutput(
        mocker.Mock(StepInput),
        PipelineProgressReporter(
            on_start_callback=on_start_callback,
            logger=mocker.Mock(),
        ),
    )

    await output.call_handling_errors(output.reporter.on_start_callback)
    output.reporter.logger.exception.assert_called_once()
