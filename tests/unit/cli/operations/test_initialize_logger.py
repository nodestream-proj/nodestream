import logging

import pytest
from hamcrest import assert_that, contains_string

from nodestream.cli.operations.initialize_logger import (
    InitializeLogger,
    configure_logging_with_json_defaults,
)
from nodestream.metrics import Metrics


@pytest.mark.asyncio
async def test_initialize_logger_json_logging_set(mocker):
    subject, command = InitializeLogger(), mocker.Mock()
    command.has_json_logging_set = True
    patch = mocker.patch(
        "nodestream.cli.operations.initialize_logger.configure_logging_with_json_defaults"
    )
    await subject.perform(command)
    patch.assert_called_once()


@pytest.mark.asyncio
async def test_initialize_logger_json_logging_unset(mocker):
    subject, command = InitializeLogger(), mocker.Mock()
    command.has_json_logging_set = False
    patch = mocker.patch(
        "nodestream.cli.operations.initialize_logger.configure_logging_with_json_defaults"
    )
    await subject.perform(command)
    patch.assert_not_called()


def test_logs_pipeline_name(capsys):
    configure_logging_with_json_defaults()
    logger = logging.getLogger("some")
    pipeline_name = "test_pipeline_name"
    scope_name = "test_scope_name"
    with Metrics.capture() as metrics:
        metrics.contextualize(scope_name, pipeline_name)
        logger.info("some message")

    captured_out = capsys.readouterr().err
    assert_that(captured_out, contains_string(pipeline_name))
    assert_that(captured_out, contains_string(scope_name))
