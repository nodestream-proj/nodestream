import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.commands.run import Run


@pytest.mark.asyncio
async def test_handle_async(mocker):
    run = Run()
    run.option = mocker.Mock(return_value=False)
    run.run_operation = mocker.AsyncMock()
    await run.handle_async()
    assert_that(run.run_operation.await_count, equal_to(4))


@pytest.mark.asyncio
async def test_auto_migrate_targets_if_needed(mocker, project_with_default_scope):
    run = Run()
    run.run_operation = mocker.AsyncMock()
    run.get_migrations = mocker.Mock()
    run.option = mocker.Mock(side_effect=[True, ["t1", "t2"]])
    project_with_default_scope.get_target_by_name = mocker.Mock()
    await run.auto_migrate_targets_if_needed(project_with_default_scope)
    assert_that(run.run_operation.await_count, equal_to(2))
