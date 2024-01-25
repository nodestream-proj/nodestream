import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.commands import RunMigrations


@pytest.mark.asyncio
async def test_handle_async(mocker, project_with_default_scope):
    run_migration = RunMigrations()
    run_migration.line = mocker.Mock()
    run_migration.option = mocker.Mock(side_effect=[None, ["t1", "t2"]])
    run_migration.run_operation = mocker.AsyncMock()
    run_migration.get_project = mocker.Mock(return_value=project_with_default_scope)
    project_with_default_scope.get_target_by_name = mocker.Mock()
    await run_migration.handle_async()
    assert_that(run_migration.run_operation.await_count, equal_to(2))


@pytest.mark.asyncio
async def test_handle_async_no_targets(mocker, project_with_default_scope):
    run_migration = RunMigrations()
    run_migration.info = mocker.Mock()
    run_migration.option = mocker.Mock(side_effect=[None, []])
    run_migration.get_project = mocker.Mock(return_value=project_with_default_scope)
    await run_migration.handle_async()
    run_migration.info.assert_called_once_with("No targets specified, nothing to do.")
