import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.commands import ExplainSchema


@pytest.mark.asyncio
async def test_handle_async_invalid_kind_returns_error_code(mocker):
    command = ExplainSchema()
    # Positional KIND/NAME mode
    command.argument = mocker.Mock(side_effect=["invalid", "TypeName"])
    command.option = mocker.Mock(side_effect=[None, None, None])
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
async def test_handle_async_valid_positional_kind_invokes_operation(mocker):
    command = ExplainSchema()
    command.argument = mocker.Mock(side_effect=["node", "Person"])
    # scope, node, relationship
    command.option = mocker.Mock(side_effect=["scope1", None, None])
    command.run_operation = mocker.AsyncMock()

    result = await command.handle_async()

    assert_that(result, equal_to(0))
    # First call: InitializeProject, second call: ExplainProjectSchema
    assert_that(command.run_operation.await_count, equal_to(2))
    operation = command.run_operation.call_args.args[0]
    assert_that(operation.node_type_name, equal_to("Person"))
    assert_that(operation.relationship_type_name, equal_to(None))
    assert_that(operation.scope, equal_to("scope1"))


@pytest.mark.asyncio
async def test_handle_async_valid_positional_relationship_invokes_operation(
    mocker,
):
    command = ExplainSchema()
    command.argument = mocker.Mock(side_effect=["relationship", "LIKES"])
    # scope, node, relationship
    command.option = mocker.Mock(side_effect=["scope1", None, None])
    command.run_operation = mocker.AsyncMock()

    result = await command.handle_async()

    assert_that(result, equal_to(0))
    # First call: InitializeProject, second call: ExplainProjectSchema
    assert_that(command.run_operation.await_count, equal_to(2))
    operation = command.run_operation.call_args.args[0]
    assert_that(operation.node_type_name, equal_to(None))
    assert_that(operation.relationship_type_name, equal_to("LIKES"))
    assert_that(operation.scope, equal_to("scope1"))


@pytest.mark.asyncio
async def test_handle_async_prefers_options_over_positional_when_both_given(
    mocker,
):
    command = ExplainSchema()
    # Positional KIND/NAME is provided, but options should take precedence.
    command.argument = mocker.Mock(side_effect=["node", "PersonPositional"])
    # scope, node (for positional gating), node (effective), relationship
    command.option = mocker.Mock(
        side_effect=["scope1", "PersonFromOption", "PersonFromOption", None]
    )
    command.run_operation = mocker.AsyncMock()

    result = await command.handle_async()

    assert_that(result, equal_to(0))
    # First call: InitializeProject, second call: ExplainProjectSchema
    assert_that(command.run_operation.await_count, equal_to(2))
    operation = command.run_operation.call_args.args[0]
    assert_that(operation.node_type_name, equal_to("PersonFromOption"))
    assert_that(operation.relationship_type_name, equal_to(None))
    assert_that(operation.scope, equal_to("scope1"))


@pytest.mark.asyncio
async def test_handle_async_node_and_relationship_options_invoke_intersection(
    mocker,
):
    command = ExplainSchema()
    # No positional KIND/NAME provided
    command.argument = mocker.Mock(side_effect=[None, None])
    # scope, node, relationship
    command.option = mocker.Mock(side_effect=["scope1", "Person", "LIKES"])
    command.run_operation = mocker.AsyncMock()

    result = await command.handle_async()

    assert_that(result, equal_to(0))
    assert_that(command.run_operation.await_count, equal_to(2))
    operation = command.run_operation.call_args.args[0]
    assert_that(operation.node_type_name, equal_to("Person"))
    assert_that(operation.relationship_type_name, equal_to("LIKES"))
    assert_that(operation.scope, equal_to("scope1"))


@pytest.mark.asyncio
async def test_handle_async_requires_some_type_information(mocker):
    command = ExplainSchema()
    command.argument = mocker.Mock(side_effect=[None, None])
    # scope, node, relationship
    command.option = mocker.Mock(side_effect=["scope1", None, None])
    command.run_operation = mocker.AsyncMock()
    command.line_error = mocker.Mock()

    result = await command.handle_async()

    assert_that(result, equal_to(1))
    command.line_error.assert_called_once()
    # Only InitializeProject should have been scheduled
    assert_that(command.run_operation.await_count, equal_to(1))
