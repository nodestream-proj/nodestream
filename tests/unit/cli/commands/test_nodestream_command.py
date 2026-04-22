import pytest

from nodestream.cli.commands.nodestream_command import NodestreamCommand


class DummyCommand(NodestreamCommand):
    async def handle_async(self):  # pragma: no cover - not used directly
        raise NotImplementedError


def test_is_verbose_and_is_very_verbose_properties(mocker):
    command = DummyCommand()
    io = mocker.Mock()
    output = mocker.Mock()
    output.is_verbose.return_value = True
    output.is_very_verbose.return_value = False
    io.output = output
    command._io = io

    assert command.is_verbose is True
    assert command.is_very_verbose is False


@pytest.mark.asyncio
async def test_async_command_run_operation(mocker):
    command, operation = NodestreamCommand(), mocker.AsyncMock()
    command.line = mocker.Mock()
    await command.run_operation(operation)
    command.line.assert_called_once()
    operation.perform.assert_awaited_once_with(command)
