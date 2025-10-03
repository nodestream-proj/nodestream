import pytest

from nodestream.pipeline.pipeline import RecordContext
from nodestream.pipeline.step import Step


class TestStep(Step):
    """Test step that tracks finalize_record calls."""

    def __init__(self):
        self.finalize_calls = []

    async def finalize_record(self, record_token):
        self.finalize_calls.append(record_token)


class ResourceTrackingStep(Step):
    """Step that tracks resources allocated per record."""

    def __init__(self):
        self.allocated_resources = {}
        self.finalized_tokens = []

    async def process_record(self, record, context):
        # Simulate allocating resources for this record
        token = f"resource_for_{id(record)}"
        self.allocated_resources[token] = f"resource_data_{id(record)}"
        yield (record, token)  # Emit tuple with callback token

    async def finalize_record(self, record_token):
        """Clean up resources allocated for this record."""
        if record_token in self.allocated_resources:
            del self.allocated_resources[record_token]
        self.finalized_tokens.append(record_token)


@pytest.mark.asyncio
async def test_step_finalize_record_default_behavior():
    """Test that default finalize_record implementation does nothing."""
    step = Step()

    # Should not raise any exceptions
    await step.finalize_record("any_token")
    await step.finalize_record(None)
    await step.finalize_record({"complex": "token"})


@pytest.mark.asyncio
async def test_step_finalize_record_custom_implementation():
    """Test that custom finalize_record implementation is called."""
    step = TestStep()

    tokens = ["token1", "token2", {"complex": "token"}]

    for token in tokens:
        await step.finalize_record(token)

    assert step.finalize_calls == tokens


@pytest.mark.asyncio
async def test_finalize_record_called_on_record_drop():
    """Test that finalize_record is called when a record is dropped."""
    step = TestStep()
    token = "test_token"

    record = RecordContext(
        record="test_data", originating_step=step, callback_token=token
    )

    await record.drop()

    assert step.finalize_calls == [token]


@pytest.mark.asyncio
async def test_finalize_record_called_with_correct_token():
    """Test that finalize_record is called with the correct callback token."""
    step = TestStep()

    # Test with simple data (token == data)
    record1 = RecordContext.from_step_emission(step, "simple_data")
    await record1.drop()

    # Test with tuple (data, token)
    record2 = RecordContext.from_step_emission(step, ("data", "custom_token"))
    await record2.drop()

    assert step.finalize_calls == ["simple_data", "custom_token"]


@pytest.mark.asyncio
async def test_finalize_record_resource_cleanup():
    """Test finalize_record for resource cleanup scenario."""
    step = ResourceTrackingStep()

    # Simulate processing records
    record1_data = "record1"
    record2_data = "record2"

    # Process first record
    async for emission in step.process_record(record1_data, None):
        record1 = RecordContext.from_step_emission(step, emission)
        break

    # Process second record
    async for emission in step.process_record(record2_data, None):
        record2 = RecordContext.from_step_emission(step, emission)
        break

    # Verify resources were allocated
    assert len(step.allocated_resources) == 2

    # Drop first record
    await record1.drop()

    # Verify first record's resources were cleaned up
    assert len(step.allocated_resources) == 1
    assert len(step.finalized_tokens) == 1

    # Drop second record
    await record2.drop()

    # Verify all resources were cleaned up
    assert len(step.allocated_resources) == 0
    assert len(step.finalized_tokens) == 2


@pytest.mark.asyncio
async def test_finalize_record_exception_handling():
    """Test behavior when finalize_record raises an exception."""

    class FailingStep(Step):
        async def finalize_record(self, record_token):
            raise ValueError(f"Failed to finalize {record_token}")

    step = FailingStep()
    record = RecordContext(
        record="test", originating_step=step, callback_token="failing_token"
    )

    # Exception should propagate
    with pytest.raises(ValueError, match="Failed to finalize failing_token"):
        await record.drop()


@pytest.mark.asyncio
async def test_finalize_record_with_child_records():
    """Test finalize_record behavior with parent-child record relationships."""
    parent_step = TestStep()
    child_step = TestStep()

    parent_record = RecordContext(
        record="parent",
        originating_step=parent_step,
        callback_token="parent_token",
        child_record_count=2,
    )

    child1 = RecordContext(
        record="child1",
        originating_step=child_step,
        callback_token="child1_token",
        originated_from=parent_record,
    )

    child2 = RecordContext(
        record="child2",
        originating_step=child_step,
        callback_token="child2_token",
        originated_from=parent_record,
    )

    # Drop first child
    await child1.drop()

    # Only child should be finalized, not parent yet
    assert child_step.finalize_calls == ["child1_token"]
    assert parent_step.finalize_calls == []

    # Drop second child
    await child2.drop()

    # Now both children and parent should be finalized
    assert child_step.finalize_calls == ["child1_token", "child2_token"]
    assert parent_step.finalize_calls == ["parent_token"]


@pytest.mark.asyncio
async def test_finalize_record_async_behavior():
    """Test that finalize_record properly handles async operations."""

    class AsyncStep(Step):
        def __init__(self):
            self.finalized_tokens = []
            self.delay_called = False

        async def finalize_record(self, record_token):
            # Simulate async cleanup work
            import asyncio

            await asyncio.sleep(0.001)  # Small delay
            self.delay_called = True
            self.finalized_tokens.append(record_token)

    step = AsyncStep()
    record = RecordContext(
        record="async_test", originating_step=step, callback_token="async_token"
    )

    await record.drop()

    assert step.delay_called
    assert step.finalized_tokens == ["async_token"]


@pytest.mark.asyncio
async def test_finalize_record_multiple_tokens_same_step():
    """Test finalize_record called multiple times on same step."""
    step = TestStep()

    records = []
    for i in range(5):
        record = RecordContext(
            record=f"data_{i}", originating_step=step, callback_token=f"token_{i}"
        )
        records.append(record)

    # Drop all records
    for record in records:
        await record.drop()

    expected_tokens = [f"token_{i}" for i in range(5)]
    assert step.finalize_calls == expected_tokens


@pytest.mark.asyncio
async def test_finalize_record_with_none_token():
    """Test finalize_record behavior with None token."""
    step = TestStep()

    record = RecordContext(record="test", originating_step=step, callback_token=None)

    await record.drop()

    assert step.finalize_calls == [None]


@pytest.mark.asyncio
async def test_finalize_record_with_complex_token():
    """Test finalize_record with complex token objects."""
    step = TestStep()

    complex_token = {
        "id": "record_123",
        "metadata": {"type": "test", "priority": 1},
        "resources": ["file1.txt", "file2.txt"],
    }

    record = RecordContext(
        record="test", originating_step=step, callback_token=complex_token
    )

    await record.drop()

    assert step.finalize_calls == [complex_token]
