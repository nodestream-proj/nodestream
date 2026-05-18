import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.commands import PrintSchema


@pytest.mark.asyncio
async def test_handle_async_uses_options_and_arguments(mocker):
    command = PrintSchema()
    command.option = mocker.Mock(side_effect=["yaml", "overrides.yaml", "out.yaml"])
    command.argument = mocker.Mock(return_value=["pipe1", "pipe2"])
    command.run_operation = mocker.AsyncMock()

    result = await command.handle_async()

    assert_that(result, equal_to(0))
    # First call: InitializeProject, second call: PrintProjectSchema
    assert_that(command.run_operation.await_count, equal_to(2))
    operation = command.run_operation.call_args.args[0]
    assert_that(operation.format_string, equal_to("yaml"))
    assert_that(operation.type_overrides_file, equal_to("overrides.yaml"))
    assert_that(operation.output_file, equal_to("out.yaml"))
    assert_that(operation.pipeline_names, equal_to(["pipe1", "pipe2"]))
