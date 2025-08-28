from unittest.mock import AsyncMock, Mock

import pytest

from nodestream.pipeline.channel import channel
from nodestream.pipeline.pipeline import (
    EmitOutstandingRecordsState,
    EmitResult,
    Exectutor,
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

    executor = Exectutor.for_step(
        mock_step, input_channel, output_channel, mock_context
    )

    assert isinstance(executor.state, StartStepState)


@pytest.mark.asyncio
async def test_executor_pipeline_output():
    input_channel, _ = channel(10)
    reporter = PipelineProgressReporter()

    executor = Exectutor.pipeline_output(input_channel, reporter)

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
