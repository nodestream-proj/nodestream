import pytest

from nodestream.cli.commands import NodestreamCommand
from nodestream.cli.operations import ExecuteMigrations
from nodestream.project import Target
from nodestream.schema.migrations import Migration, ProjectMigrations


@pytest.mark.asyncio
async def test_execute_migration_perform(mocker):
    async def test_migrations():
        yield Migration("test", [], [])
        yield Migration("test2", [], [])

    command = mocker.MagicMock(NodestreamCommand)
    migrations = mocker.Mock(ProjectMigrations)
    migrations.execute_pending.return_value = test_migrations()
    target = mocker.Mock(Target)
    subject = ExecuteMigrations(migrations, target)
    await subject.perform(command)
