import pytest

from nodestream.pipeline import Extractor
from nodestream.pipeline.channel import Channel
from nodestream.pipeline.object_storage import NullObjectStore
from nodestream.pipeline.pipeline import (
    Pipeline,
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


class TestExtractor(Extractor):
    async def extract_records(self):
        return None


class TestStep2(Step):
    async def process_record(self, record, context: StepContext):
        yield record


@pytest.mark.asyncio
async def test_pipeline_channels_obtain_input_and_output_names_from_steps(mocker):
    channel = mocker.Mock(Channel)
    mock_input = mocker.Mock(StepInput, channel=channel)
    mock_output = mocker.Mock(StepOutput, channel=channel)
    mock_input.get.return_value = None
    mock_output.put.return_value = True
    with mocker.patch(
        "nodestream.pipeline.pipeline.channel", return_value=(mock_input, mock_output)
    ):
        step1 = TestExtractor()
        step2 = TestStep2()
        pipeline = Pipeline(
            steps=[step1, step2], step_outbox_size=1, object_store=NullObjectStore()
        )
        await pipeline.run(
            reporter=PipelineProgressReporter(
                on_start_callback=lambda: None, logger=mocker.Mock()
            )
        )
        assert mock_input.register.call_args_list == [
            mocker.call("TestStep2"),
            mocker.call("TestExtractor"),
        ]
        assert mock_output.register.call_args_list == [
            mocker.call("TestStep2"),
            mocker.call("TestExtractor"),
        ]
