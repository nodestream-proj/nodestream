import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.commands.run import Run


@pytest.mark.asyncio
async def test_handle_async(mocker):
    run = Run()
    run.run_operation = mocker.AsyncMock()
    await run.handle_async()
    assert_that(run.run_operation.await_count, equal_to(3))
