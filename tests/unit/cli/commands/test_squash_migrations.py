import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.commands import SquashMigration


@pytest.mark.asyncio
async def test_show_handle_async(mocker):
    squash = SquashMigration()
    squash.run_operation = mocker.AsyncMock()
    squash.get_migrations = mocker.Mock()
    squash.option = mocker.Mock(
        side_effect=["from_migration_name", "to_migration_name"]
    )
    await squash.handle_async()

    assert_that(squash.run_operation.await_count, equal_to(1))
