import pytest

from nodestream.cli.operations import InitializeLogger


@pytest.mark.asyncio
async def test_initialize_logger_json_logging_set(mocker):
    subject, command = InitializeLogger(), mocker.Mock()
    command.option.return_value = True
    patch = mocker.patch(
        "nodestream.cli.operations.initialize_logger.configure_logging_with_json_defaults"
    )
    await subject.perform(command)
    patch.assert_called_once()


@pytest.mark.asyncio
async def test_initialize_logger_json_logging_unset(mocker):
    subject, command = InitializeLogger(), mocker.Mock()
    command.option.return_value = False
    patch = mocker.patch(
        "nodestream.cli.operations.initialize_logger.configure_logging_with_json_defaults"
    )
    await subject.perform(command)
    patch.assert_not_called()
