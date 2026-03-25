import pytest

from nodestream.cli.commands import NodestreamCommand
from nodestream.cli.operations import ExecuteMigrations
from nodestream.project import Target
from nodestream.schema.migrations import Migration, ProjectMigrations


@pytest.mark.asyncio
async def test_execute_migration_perform_with_json_logging(mocker):
    async def test_migrations():
        yield Migration("test", [], [])
        yield Migration("test2", [], [])

    command = mocker.MagicMock(NodestreamCommand)
    command.has_json_logging_set = True
    migrations = mocker.Mock(ProjectMigrations)
    migrations.execute_pending.return_value = test_migrations()
    target = mocker.Mock(Target)
    subject = ExecuteMigrations(migrations, target)
    await subject.perform(command)


@pytest.mark.asyncio
async def test_execute_migration_perform_with_spinner(mocker):
    async def test_migrations():
        yield Migration("test", [], [])
        yield Migration("test2", [], [])

    command = mocker.MagicMock(NodestreamCommand)
    command.has_json_logging_set = False

    # Mock the spin context manager to track messages.
    indicator = mocker.MagicMock()
    spin_cm = mocker.MagicMock()
    spin_cm.__enter__.return_value = indicator
    spin_cm.__exit__.return_value = False
    command.spin.return_value = spin_cm

    migrations = mocker.Mock(ProjectMigrations)
    migrations.execute_pending.return_value = test_migrations()
    target = mocker.Mock(Target)
    target.name = "test-target"

    subject = ExecuteMigrations(migrations, target)
    await subject.perform(command)

    # Ensure the spinner was created with the expected messages.
    command.spin.assert_called_once()
    # And that a message was set for at least one migration.
    indicator.set_message.assert_any_call("Migration test executed successfully.")
