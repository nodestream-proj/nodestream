import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.commands import Show


@pytest.mark.asyncio
async def test_show_handle_error_for_invalid_scope(mocker):
    show = Show()
    show.run_operation = mocker.AsyncMock()
    show.option = mocker.Mock(return_value=False)  # json=False
    # Simulate scope option being provided via property access
    type(show).scope = property(lambda self: "invalid-scope")
    # Make project.get_pipelines_schema raise a KeyError when called with this
    # scope
    project = mocker.Mock()
    project.get_pipelines_schema.side_effect = KeyError("invalid-scope")
    show.run_operation.return_value = project

    result = await show.handle_async()

    assert result == 0


@pytest.mark.asyncio
async def test_show_handle_async(mocker):
    show = Show()
    show.option = mocker.Mock()
    show.argument = mocker.Mock()
    show.run_operation = mocker.AsyncMock()
    result = await show.handle_async()
    assert_that(result, equal_to(0))
    assert_that(show.run_operation.await_count, equal_to(2))
