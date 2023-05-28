import pytest

from nodestream.cli.commands.new_project import NewProject


@pytest.mark.asyncio
async def test_handle_async(mocker):
    run = NewProject()
    run.option = mocker.Mock()
    run.argument = mocker.Mock(return_value="some/path")
    run.run_operation = mocker.AsyncMock()
    await run.handle_async()
    assert run.run_operation.await_count == 3
