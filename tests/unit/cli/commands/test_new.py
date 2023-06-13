import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.commands.new import New


@pytest.mark.asyncio
async def test_handle_async(mocker):
    run = New()
    run.option = mocker.Mock()
    run.argument = mocker.Mock(return_value="some/path")
    run.run_operation = mocker.AsyncMock()
    run.line = mocker.Mock()
    await run.handle_async()
    assert_that(run.run_operation.await_count, equal_to(4))
