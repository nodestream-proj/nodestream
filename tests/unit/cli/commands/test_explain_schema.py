import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.commands import ExplainSchema


@pytest.mark.asyncio
async def test_handle_async_invalid_kind_returns_error_code(mocker):
    command = ExplainSchema()
    command.argument = mocker.Mock(side_effect=["invalid", "TypeName"])
    command.option = mocker.Mock(return_value=None)
    command.run_operation = mocker.AsyncMock()
    command.line_error = mocker.Mock()

    result = await command.handle_async()

    assert_that(result, equal_to(1))
    command.line_error.assert_called_once_with(
        "Kind must be either 'node' or 'relationship'."
    )
    # Only the InitializeProject operation should have been scheduled
    assert_that(command.run_operation.await_count, equal_to(1))


@pytest.mark.asyncio
async def test_handle_async_valid_kind_invokes_operation(mocker):
    command = ExplainSchema()
    command.argument = mocker.Mock(side_effect=["node", "Person"])
    command.option = mocker.Mock(return_value="scope1")
    command.run_operation = mocker.AsyncMock()

    result = await command.handle_async()

    assert_that(result, equal_to(0))
    # First call: InitializeProject, second call: ExplainProjectSchema
    assert_that(command.run_operation.await_count, equal_to(2))
    _, call_kwargs = command.run_operation.call_args
    operation = command.run_operation.call_args.args[0]
    assert_that(operation.kind, equal_to("node"))
    assert_that(operation.type_name, equal_to("Person"))
    assert_that(operation.scope, equal_to("scope1"))
