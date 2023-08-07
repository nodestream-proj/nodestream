import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.commands import Remove
from nodestream.project import Project


@pytest.mark.asyncio
async def test_remove_handle_async(mocker, project_dir):
    remove = Remove()
    remove.argument = mocker.Mock()
    remove.option = mocker.Mock()
    remove.get_project = mocker.Mock(return_value=mocker.Mock(Project))
    remove.run_operation = mocker.AsyncMock()
    remove.get_project_path = mocker.Mock(return_value=project_dir)
    await remove.handle_async()
    assert_that(remove.run_operation.await_count, equal_to(3))
