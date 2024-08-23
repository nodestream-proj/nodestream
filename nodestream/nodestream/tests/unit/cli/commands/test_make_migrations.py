import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.commands import MakeMigration


@pytest.mark.asyncio
async def test_handle_async(mocker, project_with_default_scope, basic_schema):
    make_migration = MakeMigration()
    make_migration.option = mocker.Mock(return_value=None)
    make_migration.run_operation = mocker.AsyncMock()
    make_migration.get_project = mocker.Mock(return_value=project_with_default_scope)
    project_with_default_scope.get_schema = mocker.Mock(return_value=basic_schema)
    await make_migration.handle_async()
    assert_that(make_migration.run_operation.await_count, equal_to(1))
