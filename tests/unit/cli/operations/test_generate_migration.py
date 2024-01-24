import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.commands import NodestreamCommand
from nodestream.cli.operations import GenerateMigration
from nodestream.schema.migrations import Migration, ProjectMigrations


@pytest.mark.asyncio
async def test_generate_migration_perform(mocker, basic_schema, project_dir):
    command = mocker.MagicMock(NodestreamCommand)
    migrations = mocker.Mock(ProjectMigrations)
    generated_migration_and_path = Migration("test", [], []), project_dir
    migrations.create_migration_from_changes = mocker.AsyncMock(
        return_value=generated_migration_and_path
    )
    subject = GenerateMigration(migrations, basic_schema)
    await subject.perform(command)

    assert_that(command.line.call_count, equal_to(4))
