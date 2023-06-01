import pytest

from nodestream.cli.commands import Show


@pytest.mark.asyncio
async def test_show_handle_asnyc(mocker):
    show = Show()
    show.option = mocker.Mock()
    show.argument = mocker.Mock()
    show.run_operation = mocker.AsyncMock()
    await show.handle_async()
    assert show.run_operation.await_count == 2
