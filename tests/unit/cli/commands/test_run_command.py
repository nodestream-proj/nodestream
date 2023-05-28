import pytest

from nodestream.cli.commands.run_command import Run


@pytest.mark.asyncio
async def test_handle_async(mocker):
    run = Run()
    run.run_operation = mocker.AsyncMock()
    await run.handle_async()
    assert run.run_operation.await_count == 3
