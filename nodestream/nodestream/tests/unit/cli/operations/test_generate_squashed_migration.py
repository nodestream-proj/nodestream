import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.commands import NodestreamCommand
from nodestream.cli.operations import GenerateSquashedMigration
from nodestream.schema.migrations import Migration
from nodestream.schema.migrations.operations import CreateNodeType


@pytest.mark.asyncio
async def test_generate_squash_migration_perform(mocker, project_dir):
    command = mocker.MagicMock(NodestreamCommand)
    migrations = mocker.Mock()
    op = CreateNodeType("Person", ["ssn"], [])
    generated_migration_and_path = Migration("test", [op], []), project_dir
    migrations.create_squash_between = mocker.Mock(
        return_value=generated_migration_and_path
    )
    subject = GenerateSquashedMigration(migrations, "from", "to")
    await subject.perform(command)

    assert_that(command.line.call_count, equal_to(5))
