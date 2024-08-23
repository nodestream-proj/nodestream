import pytest

from nodestream.pipeline.progress_reporter import PipelineProgressReporter
from nodestream.pipeline.step import Step, StepContext


@pytest.mark.asyncio
async def test_default_start_does_nothing(mocker):
    step = Step()
    ctx = mocker.Mock(spec=StepContext)
    await step.start(ctx)
    ctx.assert_not_called()


@pytest.mark.asyncio
async def test_default_finish_does_nothing(mocker):
    step = Step()
    ctx = mocker.Mock(spec=StepContext)
    await step.finish(ctx)
    ctx.assert_not_called()


@pytest.mark.asyncio
async def test_default_emit_outstanding_records_does_nothing(mocker):
    step = Step()
    async for _ in step.emit_outstanding_records():
        assert False, "Should not emit any records"


@pytest.mark.asyncio
async def test_default_process_record_yields_input_record(mocker):
    step = Step()
    ctx = mocker.Mock(spec=StepContext)
    record = object()
    async for output_record in step.process_record(record, ctx):
        assert output_record is record


@pytest.mark.asyncio
async def test_step_context_report_error(mocker):
    exception = Exception()
    ctx = StepContext(
        "bob", 1, PipelineProgressReporter(on_fatal_error_callback=mocker.Mock())
    )
    ctx.report_error("oh no, an error!", exception)
    ctx.reporter.on_fatal_error_callback.assert_not_called()


@pytest.mark.asyncio
async def test_step_context_report_fatal_error(mocker):
    exception = Exception()
    ctx = StepContext(
        "bob", 1, PipelineProgressReporter(on_fatal_error_callback=mocker.Mock())
    )
    ctx.report_error("oh no, a fatal error!", exception, fatal=True)
    ctx.reporter.on_fatal_error_callback.assert_called_once_with(exception)


@pytest.mark.asyncio
async def test_step_context_report_debug_message(mocker):
    ctx = StepContext("bob", 1, PipelineProgressReporter(logger=mocker.Mock()))
    ctx.debug("debug message", x=12)
    ctx.reporter.logger.debug.assert_called_once_with(
        "debug message", extra={"index": 1, "x": 12, "step_name": "bob"}
    )


@pytest.mark.asyncio
async def test_step_context_report_info_message(mocker):
    ctx = StepContext("bob", 1, PipelineProgressReporter(logger=mocker.Mock()))
    ctx.info("info message", x=12)
    ctx.reporter.logger.info.assert_called_once_with(
        "info message", extra={"index": 1, "x": 12, "step_name": "bob"}
    )


@pytest.mark.asyncio
async def test_step_context_report_warning_message(mocker):
    ctx = StepContext("bob", 1, PipelineProgressReporter(logger=mocker.Mock()))
    ctx.warning("warning message", x=12)
    ctx.reporter.logger.warning.assert_called_once_with(
        "warning message", extra={"index": 1, "x": 12, "step_name": "bob"}
    )
