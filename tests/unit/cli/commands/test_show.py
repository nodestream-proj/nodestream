import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.commands import Show


@pytest.mark.asyncio
async def test_show_handle_async(mocker):
    show = Show()
    show.option = mocker.Mock()
    show.argument = mocker.Mock()
    show.run_operation = mocker.AsyncMock()
    await show.handle_async()
    assert_that(show.run_operation.await_count, equal_to(2))
