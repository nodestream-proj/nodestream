from unittest.mock import AsyncMock, Mock

import pytest

from nodestream.pipeline.channel import channel
from nodestream.pipeline.pipeline import (
    EmitOutstandingRecordsState,
    EmitResult,
    Executor,
    Pipeline,
    PipelineOutputProcessRecordsState,
    PipelineOutputStartState,
    PipelineOutputStopState,
    ProcessRecordsState,
    Record,
    StartStepState,
    StepExecutionState,
    StopStepExecution,
)
from nodestream.pipeline.progress_reporter import PipelineProgressReporter
from nodestream.pipeline.step import Step, StepContext


@pytest.fixture
def mock_step():
    step = Mock(spec=Step)
    step.start = AsyncMock()
    step.finish = AsyncMock()
    step.process_record = AsyncMock()
    step.emit_outstanding_records = AsyncMock()
    step.finalize_record = AsyncMock()
    return step


@pytest.fixture
def mock_context():
    context = Mock(spec=StepContext)
    context.report_error = Mock()
    context.debug = Mock()
    return context


@pytest.fixture
def step_execution_state(mock_step, mock_context):
    input_channel, output_channel = channel(10)
    return StepExecutionState(mock_step, mock_context, input_channel, output_channel)


# Tests for Record class
@pytest.mark.asyncio
async def test_record_from_step_emission_simple():
    step = Mock(spec=Step)
    data = {"test": "data"}

    record = Record.from_step_emission(step, data)

    assert record.data == data
    assert record.callback_token == data
    assert record.originating_step == step
    assert record.originated_from is None
    assert record.child_record_count == 0


@pytest.mark.asyncio
async def test_record_from_step_emission_with_tuple():
    step = Mock(spec=Step)
    data = {"test": "data"}
    token = "callback_token"

    record = Record.from_step_emission(step, (data, token))

    assert record.data == data
    assert record.callback_token == token
    assert record.originating_step == step


@pytest.mark.asyncio
async def test_record_drop_calls_finalize():
    step = Mock(spec=Step)
    step.finalize_record = AsyncMock()
    token = "test_token"

    record = Record(data="test", originating_step=step, callback_token=token)
    await record.drop()

    step.finalize_record.assert_called_once_with(token)


@pytest.mark.asyncio
async def test_record_drop_propagates_to_parent():
    parent_step = Mock(spec=Step)
    parent_step.finalize_record = AsyncMock()
    child_step = Mock(spec=Step)
    child_step.finalize_record = AsyncMock()

    parent_record = Record(
        data="parent", originating_step=parent_step, callback_token="parent_token"
    )
    child_record = Record(
        data="child",
        originating_step=child_step,
        callback_token="child_token",
        originated_from=parent_record,
    )

    await child_record.drop()

    child_step.finalize_record.assert_called_once_with("child_token")
    # Parent should have child_dropped called
    assert parent_record.child_record_count == -1


@pytest.mark.asyncio
async def test_record_child_dropped():
    step = Mock(spec=Step)
    step.finalize_record = AsyncMock()

    record = Record(
        data="test", originating_step=step, callback_token="token", child_record_count=1
    )
    await record.child_dropped()

    assert record.child_record_count == 0
    step.finalize_record.assert_called_once_with("token")


# Tests for EmitResult enum
def test_emit_result_should_continue():
    assert EmitResult.EMITTED_RECORDS.should_continue is True
    assert EmitResult.NO_OP.should_continue is True
    assert EmitResult.CLOSED_DOWNSTREAM.should_continue is False


def test_emit_result_did_emit_records():
    assert EmitResult.EMITTED_RECORDS.did_emit_records is True
    assert EmitResult.NO_OP.did_emit_records is False
    assert EmitResult.CLOSED_DOWNSTREAM.did_emit_records is False


# Tests for StartStepState
@pytest.mark.asyncio
async def test_start_step_state_success(mock_step, mock_context):
    input_channel, output_channel = channel(10)
    state = StartStepState(mock_step, mock_context, input_channel, output_channel)

    next_state = await state.execute_until_state_change()

    mock_step.start.assert_called_once_with(mock_context)
    assert isinstance(next_state, ProcessRecordsState)


@pytest.mark.asyncio
async def test_start_step_state_error(mock_step, mock_context):
    input_channel, output_channel = channel(10)
    state = StartStepState(mock_step, mock_context, input_channel, output_channel)
    mock_step.start.side_effect = Exception("Start failed")

    next_state = await state.execute_until_state_change()

    mock_context.report_error.assert_called_once()
    assert next_state is None


# Tests for ProcessRecordsState
@pytest.mark.asyncio
async def test_process_records_state_transitions_to_emit_outstanding():
    """Test that ProcessRecordsState transitions to EmitOutstandingRecordsState."""
    mock_step = Mock(spec=Step)
    mock_context = Mock(spec=StepContext)

    # Create mock channels
    mock_input = Mock()
    mock_input.get = AsyncMock(side_effect=[None])  # No records, just end signal

    mock_output = Mock()

    state = ProcessRecordsState(mock_step, mock_context, mock_input, mock_output)

    next_state = await state.execute_until_state_change()

    assert isinstance(next_state, EmitOutstandingRecordsState)


@pytest.mark.asyncio
async def test_process_records_state_drops_record_when_no_emission():
    mock_step = Mock(spec=Step)
    mock_context = Mock(spec=StepContext)
    input_channel, output_channel = channel(10)

    state = ProcessRecordsState(mock_step, mock_context, input_channel, output_channel)

    # Create a record with finalize_record mock
    originating_step = Mock(spec=Step)
    originating_step.finalize_record = AsyncMock()
    test_record = Record(
        data="test_data", originating_step=originating_step, callback_token="token"
    )

    # Mock process_record to return empty generator
    async def empty_generator(data, context):
        return
        yield  # pragma: no cover

    mock_step.process_record.return_value = empty_generator("test_data", mock_context)

    # Put record and end signal
    await input_channel.channel.put(test_record)
    await input_channel.channel.put(None)

    await state.execute_until_state_change()

    # Verify record was dropped (finalize_record called)
    originating_step.finalize_record.assert_called_once_with("token")


# Tests for StopStepExecution
@pytest.mark.asyncio
async def test_stop_step_execution(mock_step, mock_context):
    input_channel, output_channel = channel(10)
    state = StopStepExecution(mock_step, mock_context, input_channel, output_channel)

    next_state = await state.execute_until_state_change()

    mock_step.finish.assert_called_once_with(mock_context)
    assert next_state is None


# Tests for Executor
@pytest.mark.asyncio
async def test_executor_for_step():
    mock_step = Mock(spec=Step)
    mock_step.start = AsyncMock()
    mock_step.finish = AsyncMock()
    input_channel, output_channel = channel(10)
    mock_context = Mock(spec=StepContext)

    executor = Executor.for_step(mock_step, input_channel, output_channel, mock_context)

    assert isinstance(executor.state, StartStepState)


@pytest.mark.asyncio
async def test_executor_pipeline_output():
    input_channel, _ = channel(10)
    reporter = PipelineProgressReporter()

    executor = Executor.pipeline_output(input_channel, reporter)

    assert isinstance(executor.state, PipelineOutputStartState)


@pytest.mark.asyncio
async def test_pipeline_output_observes_data_not_record():
    """Test that observability callback receives data, not Record objects."""
    observed_items = []

    def observe_callback(item):
        observed_items.append(item)

    input_channel, _ = channel(10)
    reporter = PipelineProgressReporter(observability_callback=observe_callback)

    # Create pipeline output state
    from nodestream.metrics import Metrics

    metrics = Metrics()
    state = PipelineOutputProcessRecordsState(input_channel, reporter, metrics)

    # Create test records with different data types
    step = Mock(spec=Step)
    test_data = ["simple_string", {"key": "value"}, ["list", "item"], 42]

    # Put records in input channel
    for data in test_data:
        record = Record(data=data, originating_step=step, callback_token=data)
        await input_channel.channel.put(record)

    # Signal end
    await input_channel.channel.put(None)

    # Execute the state
    await state.execute_until_state_change()

    # Verify that observed items are the actual data, not Record objects
    assert len(observed_items) == len(test_data)
    for i, observed in enumerate(observed_items):
        assert observed == test_data[i]
        assert not hasattr(observed, "data")  # Should not be a Record
        assert not hasattr(observed, "originating_step")  # Should not be a Record


# Tests for StepExecutionState emit_record method
@pytest.mark.asyncio
async def test_step_execution_state_emit_record_success(mock_step, mock_context):
    input_channel, output_channel = channel(10)
    state = StartStepState(mock_step, mock_context, input_channel, output_channel)

    test_record = Record(
        data="test", originating_step=mock_step, callback_token="token"
    )
    result = await state.emit_record(test_record)

    assert result == EmitResult.EMITTED_RECORDS


@pytest.mark.asyncio
async def test_step_execution_state_emit_record_closed_downstream(
    mock_step, mock_context
):
    input_channel, output_channel = channel(10)
    state = StartStepState(mock_step, mock_context, input_channel, output_channel)

    # Set input_dropped to simulate downstream not accepting records
    output_channel.channel.input_dropped = True

    test_record = Record(
        data="test", originating_step=mock_step, callback_token="token"
    )
    result = await state.emit_record(test_record)

    assert result == EmitResult.CLOSED_DOWNSTREAM
    mock_context.debug.assert_called_once_with(
        "Downstream is not accepting records. Stopping"
    )


# Tests for StepExecutionState emit_from_generator method
@pytest.mark.asyncio
async def test_step_execution_state_emit_from_generator_with_records(
    mock_step, mock_context
):
    input_channel, output_channel = channel(10)
    state = StartStepState(mock_step, mock_context, input_channel, output_channel)

    async def test_generator():
        yield "record1"
        yield "record2"

    result = await state.emit_from_generator(test_generator())

    assert result == EmitResult.EMITTED_RECORDS


@pytest.mark.asyncio
async def test_step_execution_state_emit_from_generator_closed_downstream(
    mock_step, mock_context
):
    input_channel, output_channel = channel(10)
    state = StartStepState(mock_step, mock_context, input_channel, output_channel)

    # Set input_dropped to simulate downstream not accepting records
    output_channel.channel.input_dropped = True

    async def test_generator():
        yield "record1"
        yield "record2"

    result = await state.emit_from_generator(test_generator())

    assert result == EmitResult.CLOSED_DOWNSTREAM


# Tests for ProcessRecordsState exception handling
@pytest.mark.asyncio
async def test_process_records_state_exception_handling(mock_step, mock_context):
    input_channel, output_channel = channel(10)
    state = ProcessRecordsState(mock_step, mock_context, input_channel, output_channel)

    # Create a record
    originating_step = Mock(spec=Step)
    originating_step.finalize_record = AsyncMock()
    test_record = Record(
        data="test_data", originating_step=originating_step, callback_token="token"
    )

    # Mock process_record to raise an exception directly (not return a coroutine)
    def failing_process_record(data, context):
        raise Exception("Processing failed")

    mock_step.process_record = failing_process_record

    # Put record in input channel
    await input_channel.channel.put(test_record)
    await input_channel.channel.put(None)

    next_state = await state.execute_until_state_change()

    # Check that report_error was called with the right message and fatal=True
    # The exact exception may vary, so we just check the call was made
    mock_context.report_error.assert_called_once()
    args, kwargs = mock_context.report_error.call_args
    assert args[0] == "Error processing record"
    assert kwargs.get("fatal") is True
    assert isinstance(next_state, StopStepExecution)


@pytest.mark.asyncio
async def test_process_records_state_closed_downstream_transition(
    mock_step, mock_context
):
    input_channel, output_channel = channel(10)
    state = ProcessRecordsState(mock_step, mock_context, input_channel, output_channel)

    # Set input_dropped to simulate downstream not accepting records
    output_channel.channel.input_dropped = True

    # Create a record
    originating_step = Mock(spec=Step)
    originating_step.finalize_record = AsyncMock()
    test_record = Record(
        data="test_data", originating_step=originating_step, callback_token="token"
    )

    # Mock process_record to return a generator with records
    async def test_generator(data, context):
        yield "output_record"

    mock_step.process_record.return_value = test_generator("test_data", mock_context)

    # Put record in input channel
    await input_channel.channel.put(test_record)
    await input_channel.channel.put(None)

    next_state = await state.execute_until_state_change()

    assert isinstance(next_state, StopStepExecution)


# Tests for EmitOutstandingRecordsState
@pytest.mark.asyncio
async def test_emit_outstanding_records_state_success(mock_step, mock_context):
    input_channel, output_channel = channel(10)
    state = EmitOutstandingRecordsState(
        mock_step, mock_context, input_channel, output_channel
    )

    # Mock emit_outstanding_records to return a generator
    async def outstanding_generator(context):
        yield "outstanding_record1"
        yield "outstanding_record2"

    mock_step.emit_outstanding_records.return_value = outstanding_generator(
        mock_context
    )

    next_state = await state.execute_until_state_change()

    mock_step.emit_outstanding_records.assert_called_once_with(mock_context)
    assert isinstance(next_state, StopStepExecution)


@pytest.mark.asyncio
async def test_emit_outstanding_records_state_exception_handling(
    mock_step, mock_context
):
    input_channel, output_channel = channel(10)
    state = EmitOutstandingRecordsState(
        mock_step, mock_context, input_channel, output_channel
    )

    # Mock emit_outstanding_records to raise an exception directly
    def failing_emit_outstanding(context):
        raise Exception("Outstanding records failed")

    mock_step.emit_outstanding_records = failing_emit_outstanding

    next_state = await state.execute_until_state_change()

    # Check that report_error was called with the right message and fatal=True
    mock_context.report_error.assert_called_once()
    args, kwargs = mock_context.report_error.call_args
    assert args[0] == "Error emitting outstanding records"
    assert kwargs.get("fatal") is True
    assert isinstance(next_state, StopStepExecution)


# Tests for StopStepExecution error handling
@pytest.mark.asyncio
async def test_stop_step_execution_finish_exception(mock_step, mock_context):
    input_channel, output_channel = channel(10)
    state = StopStepExecution(mock_step, mock_context, input_channel, output_channel)

    # Mock finish to raise an exception
    mock_step.finish.side_effect = Exception("Finish failed")

    next_state = await state.execute_until_state_change()

    mock_context.report_error.assert_called_once_with(
        "Error stopping step", mock_step.finish.side_effect
    )
    assert next_state is None


# Tests for PipelineOutputState call_ignoring_errors method
@pytest.mark.asyncio
async def test_pipeline_output_state_call_ignoring_errors_success():
    input_channel, _ = channel(10)
    reporter = PipelineProgressReporter()
    from nodestream.metrics import Metrics

    metrics = Metrics()

    state = PipelineOutputProcessRecordsState(input_channel, reporter, metrics)

    # Test successful callback
    callback_called = False

    def test_callback():
        nonlocal callback_called
        callback_called = True

    state.call_ignoring_errors(test_callback)
    assert callback_called


@pytest.mark.asyncio
async def test_pipeline_output_state_call_ignoring_errors_exception():
    input_channel, _ = channel(10)
    reporter = PipelineProgressReporter()
    from nodestream.metrics import Metrics

    metrics = Metrics()

    state = PipelineOutputProcessRecordsState(input_channel, reporter, metrics)

    # Test callback that raises exception
    def failing_callback():
        raise Exception("Callback failed")

    # Should not raise exception, just log it
    state.call_ignoring_errors(failing_callback)


# Tests for PipelineOutputStartState
@pytest.mark.asyncio
async def test_pipeline_output_start_state():
    input_channel, _ = channel(10)
    reporter = PipelineProgressReporter()
    from nodestream.metrics import Metrics

    metrics = Metrics()

    state = PipelineOutputStartState(input_channel, reporter, metrics)

    next_state = await state.execute_until_state_change()

    assert isinstance(next_state, PipelineOutputProcessRecordsState)


# Tests for PipelineOutputStopState
@pytest.mark.asyncio
async def test_pipeline_output_stop_state():
    input_channel, _ = channel(10)
    reporter = PipelineProgressReporter()
    from nodestream.metrics import Metrics

    metrics = Metrics()

    state = PipelineOutputStopState(input_channel, reporter, metrics)

    next_state = await state.execute_until_state_change()

    assert next_state is None


# Tests for Executor run method
@pytest.mark.asyncio
async def test_executor_run():
    mock_step = Mock(spec=Step)
    mock_step.start = AsyncMock()
    mock_step.finish = AsyncMock()
    mock_step.process_record = AsyncMock()
    mock_step.emit_outstanding_records = AsyncMock()
    mock_step.finalize_record = AsyncMock()

    input_channel, output_channel = channel(10)
    mock_context = Mock(spec=StepContext)
    mock_context.report_error = Mock()
    mock_context.debug = Mock()

    executor = Executor.for_step(mock_step, input_channel, output_channel, mock_context)

    # Mock emit_outstanding_records to return empty generator
    async def empty_generator(context):
        return
        yield  # pragma: no cover

    mock_step.emit_outstanding_records.return_value = empty_generator(mock_context)

    # Close input to end processing
    await input_channel.channel.put(None)

    await executor.run()

    mock_step.start.assert_called_once_with(mock_context)
    mock_step.finish.assert_called_once_with(mock_context)


# Tests for Pipeline class
@pytest.mark.asyncio
async def test_pipeline_init():
    from nodestream.pipeline.step import PassStep
    from nodestream.pipeline.object_storage import NullObjectStore

    steps = (PassStep(), PassStep())
    step_outbox_size = 100
    object_store = NullObjectStore()

    pipeline = Pipeline(steps, step_outbox_size, object_store)

    assert pipeline.steps == steps
    assert pipeline.step_outbox_size == step_outbox_size
    assert pipeline.object_store == object_store


@pytest.mark.asyncio
async def test_pipeline_get_child_expanders():
    from nodestream.pipeline.step import PassStep
    from nodestream.pipeline.object_storage import NullObjectStore
    from nodestream.schema import ExpandsSchema

    # Create a mock step that implements both Step and ExpandsSchema
    class MockExpanderStep(Step, ExpandsSchema):
        def process_record(self, record, context):
            yield record

        def expand_schema(self, schema):
            pass

    mock_expander_step = MockExpanderStep()
    regular_step = PassStep()

    steps = (regular_step, mock_expander_step)
    pipeline = Pipeline(steps, 100, NullObjectStore())

    expanders = list(pipeline.get_child_expanders())

    assert len(expanders) == 1
    assert expanders[0] == mock_expander_step


@pytest.mark.asyncio
async def test_pipeline_run():
    from nodestream.pipeline.step import PassStep
    from nodestream.pipeline.object_storage import NullObjectStore

    # Create simple steps
    step1 = PassStep()
    step2 = PassStep()
    steps = (step1, step2)

    pipeline = Pipeline(steps, 10, NullObjectStore())
    reporter = PipelineProgressReporter()

    # Run the pipeline
    await pipeline.run(reporter)

    # If we get here without exception, the pipeline ran successfully


# Test to cover the remaining missing lines (150-151, 184, 244)
@pytest.mark.asyncio
async def test_emit_from_generator_early_return_on_closed_downstream(
    mock_step, mock_context
):
    """Test that emit_from_generator returns early when downstream closes."""
    input_channel, output_channel = channel(10)
    state = StartStepState(mock_step, mock_context, input_channel, output_channel)

    # Create a custom generator that will set input_dropped after first yield
    call_count = 0

    async def test_generator():
        nonlocal call_count
        call_count += 1
        yield "record1"
        # After first record is yielded, simulate downstream closing
        output_channel.channel.input_dropped = True
        call_count += 1
        yield "record2"  # This should not be processed due to early return
        call_count += 1
        yield "record3"

    result = await state.emit_from_generator(test_generator())

    # Should return CLOSED_DOWNSTREAM and only process first record
    assert result == EmitResult.CLOSED_DOWNSTREAM
    # Only first record should be processed, generator should stop after that
    assert call_count == 2  # First yield + attempt at second yield


# Test to cover line 244 - the specific case where should_continue is False
@pytest.mark.asyncio
async def test_process_records_state_should_continue_false(mock_step, mock_context):
    """Test ProcessRecordsState when emit result should_continue is False."""
    input_channel, output_channel = channel(10)
    state = ProcessRecordsState(mock_step, mock_context, input_channel, output_channel)

    # Create a record
    originating_step = Mock(spec=Step)
    originating_step.finalize_record = AsyncMock()
    test_record = Record(
        data="test_data", originating_step=originating_step, callback_token="token"
    )

    # Mock process_record to return a generator that yields records
    async def test_generator(data, context):
        yield "output_record"

    mock_step.process_record.return_value = test_generator("test_data", mock_context)

    # Put record in input channel
    await input_channel.channel.put(test_record)

    # Set input_dropped after putting the record to simulate downstream closing
    # during processing (this will make should_continue False)
    output_channel.channel.input_dropped = True

    # Put end signal
    await input_channel.channel.put(None)

    next_state = await state.execute_until_state_change()

    # Should transition to StopStepExecution due to should_continue being False
    assert isinstance(next_state, StopStepExecution)
