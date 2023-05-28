import pytest

from nodestream.cli.commands.async_command import AsyncCommand


@pytest.mark.asyncio
async def test_async_command_run_operation(mocker):
    command, operation = AsyncCommand(), mocker.AsyncMock()
    command.line = mocker.Mock()
    await command.run_operation(operation)
    command.line.assert_called_once()
    operation.perform.assert_awaited_once_with(command)
