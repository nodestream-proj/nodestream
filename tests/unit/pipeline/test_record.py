from unittest.mock import AsyncMock, Mock

import pytest

from nodestream.pipeline.pipeline import RecordContext
from nodestream.pipeline.step import Step


@pytest.mark.asyncio
async def test_record_creation_simple():
    """Test basic record creation."""
    step = Mock(spec=Step)
    data = {"test": "data"}

    record = RecordContext(record=data, originating_step=step, callback_token="token")

    assert record.record == data
    assert record.originating_step == step
    assert record.callback_token == "token"
    assert record.originated_from is None
    assert record.child_record_count == 0


@pytest.mark.asyncio
async def test_record_from_step_emission_simple_data():
    """Test Record.from_step_emission with simple data."""
    step = Mock(spec=Step)
    data = {"test": "data"}

    record = RecordContext.from_step_emission(step, data)

    assert record.record == data
    assert record.callback_token == data
    assert record.originating_step == step
    assert record.originated_from is None


@pytest.mark.asyncio
async def test_record_from_step_emission_tuple_data():
    """Test Record.from_step_emission with tuple (data, token)."""
    step = Mock(spec=Step)
    data = {"test": "data"}
    token = "callback_token"

    record = RecordContext.from_step_emission(step, (data, token))

    assert record.record == data
    assert record.callback_token == token
    assert record.originating_step == step


@pytest.mark.asyncio
async def test_record_from_step_emission_with_origin():
    """Test Record.from_step_emission with originated_from."""
    step = Mock(spec=Step)
    origin_step = Mock(spec=Step)
    origin_record = RecordContext(
        record="origin", originating_step=origin_step, callback_token="origin_token"
    )

    record = RecordContext.from_step_emission(step, "new_data", origin_record)

    assert record.record == "new_data"
    assert record.originated_from == origin_record


@pytest.mark.asyncio
async def test_record_drop_calls_finalize_record():
    """Test that Record.drop() calls finalize_record on originating step."""
    step = Mock(spec=Step)
    step.finalize_record = AsyncMock()
    token = "test_token"

    record = RecordContext(record="test", originating_step=step, callback_token=token)

    await record.drop()

    step.finalize_record.assert_called_once_with(token)


@pytest.mark.asyncio
async def test_record_drop_propagates_to_parent():
    """Test that Record.drop() propagates to parent record."""
    parent_step = Mock(spec=Step)
    parent_step.finalize_record = AsyncMock()
    child_step = Mock(spec=Step)
    child_step.finalize_record = AsyncMock()

    parent_record = RecordContext(
        record="parent", originating_step=parent_step, callback_token="parent_token"
    )

    child_record = RecordContext(
        record="child",
        originating_step=child_step,
        callback_token="child_token",
        originated_from=parent_record,
    )

    await child_record.drop()

    # Child should finalize itself
    child_step.finalize_record.assert_called_once_with("child_token")

    # Parent should have child_dropped called (decrements child_record_count)
    assert parent_record.child_record_count == -1


@pytest.mark.asyncio
async def test_record_child_dropped_decrements_count():
    """Test that child_dropped decrements child_record_count."""
    step = Mock(spec=Step)
    step.finalize_record = AsyncMock()

    record = RecordContext(
        record="test",
        originating_step=step,
        callback_token="token",
        child_record_count=3,
    )

    await record.child_dropped()

    assert record.child_record_count == 2
    # Should not finalize yet since count > 0
    step.finalize_record.assert_not_called()


@pytest.mark.asyncio
async def test_record_child_dropped_finalizes_when_count_zero():
    """Test that child_dropped finalizes record when count reaches zero."""
    step = Mock(spec=Step)
    step.finalize_record = AsyncMock()

    record = RecordContext(
        record="test",
        originating_step=step,
        callback_token="token",
        child_record_count=1,
    )

    await record.child_dropped()

    assert record.child_record_count == 0
    step.finalize_record.assert_called_once_with("token")


@pytest.mark.asyncio
async def test_record_child_dropped_propagates_to_grandparent():
    """Test that child_dropped propagates up the chain."""
    grandparent_step = Mock(spec=Step)
    grandparent_step.finalize_record = AsyncMock()
    parent_step = Mock(spec=Step)
    parent_step.finalize_record = AsyncMock()
    child_step = Mock(spec=Step)
    child_step.finalize_record = AsyncMock()

    grandparent_record = RecordContext(
        record="grandparent",
        originating_step=grandparent_step,
        callback_token="grandparent_token",
        child_record_count=1,
    )

    parent_record = RecordContext(
        record="parent",
        originating_step=parent_step,
        callback_token="parent_token",
        originated_from=grandparent_record,
        child_record_count=1,
    )

    child_record = RecordContext(
        record="child",
        originating_step=child_step,
        callback_token="child_token",
        originated_from=parent_record,
    )

    await child_record.drop()

    # Child should finalize
    child_step.finalize_record.assert_called_once_with("child_token")

    # Parent should have count decremented and finalize (since it goes to 0)
    assert parent_record.child_record_count == 0
    parent_step.finalize_record.assert_called_once_with("parent_token")

    # Grandparent should have count decremented and finalize
    assert grandparent_record.child_record_count == 0
    grandparent_step.finalize_record.assert_called_once_with("grandparent_token")


@pytest.mark.asyncio
async def test_record_multiple_children_cleanup():
    """Test cleanup behavior with multiple children."""
    parent_step = Mock(spec=Step)
    parent_step.finalize_record = AsyncMock()

    parent_record = RecordContext(
        record="parent",
        originating_step=parent_step,
        callback_token="parent_token",
        child_record_count=2,  # Has 2 children
    )

    # First child drops
    await parent_record.child_dropped()
    assert parent_record.child_record_count == 1
    parent_step.finalize_record.assert_not_called()

    # Second child drops - should trigger finalization
    await parent_record.child_dropped()
    assert parent_record.child_record_count == 0
    parent_step.finalize_record.assert_called_once_with("parent_token")


@pytest.mark.asyncio
async def test_record_cleanup_with_no_parent():
    """Test that records without parents clean up properly."""
    step = Mock(spec=Step)
    step.finalize_record = AsyncMock()

    record = RecordContext(
        record="orphan", originating_step=step, callback_token="orphan_token"
    )

    await record.drop()

    step.finalize_record.assert_called_once_with("orphan_token")


@pytest.mark.asyncio
async def test_record_cleanup_exception_handling():
    """Test that exceptions in finalize_record break cleanup chain."""
    parent_step = Mock(spec=Step)
    parent_step.finalize_record = AsyncMock()
    child_step = Mock(spec=Step)
    child_step.finalize_record = AsyncMock(side_effect=Exception("Cleanup failed"))

    parent_record = RecordContext(
        record="parent",
        originating_step=parent_step,
        callback_token="parent_token",
        child_record_count=1,
    )

    child_record = RecordContext(
        record="child",
        originating_step=child_step,
        callback_token="child_token",
        originated_from=parent_record,
    )

    # Child cleanup should fail and parent should NOT be notified
    # (this is current behavior - could be improved)
    with pytest.raises(Exception, match="Cleanup failed"):
        await child_record.drop()

    # Parent should NOT have been notified due to exception
    assert parent_record.child_record_count == 1
    parent_step.finalize_record.assert_not_called()
