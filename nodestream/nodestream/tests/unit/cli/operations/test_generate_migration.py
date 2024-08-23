import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.commands import NodestreamCommand
from nodestream.cli.operations.generate_migration import (
    CleoMigrationInput,
    GenerateMigration,
)
from nodestream.schema.migrations import Migration, ProjectMigrations
from nodestream.schema.migrations.operations import CreateNodeType


@pytest.mark.asyncio
async def test_generate_migration_perform(mocker, basic_schema, project_dir):
    command = mocker.MagicMock(NodestreamCommand)
    migrations = mocker.Mock(ProjectMigrations)
    op = CreateNodeType("Person", ["ssn"], [])
    generated_migration_and_path = Migration("test", [op], []), project_dir
    migrations.create_migration_from_changes = mocker.AsyncMock(
        return_value=generated_migration_and_path
    )
    subject = GenerateMigration(migrations, basic_schema)
    await subject.perform(command)

    assert_that(command.line.call_count, equal_to(5))


@pytest.mark.asyncio
async def test_generate_migration_perform_no_resultant_migration(mocker):
    command = mocker.MagicMock(NodestreamCommand)
    subject = GenerateMigration(None, None, False)
    subject.generate_migration = mocker.AsyncMock(return_value=(None, None))
    await subject.perform(command)
    command.line.assert_called_once_with("No changes to migrate")


@pytest.mark.asyncio
async def test_generate_migration_perform_dry_run(mocker):
    command = mocker.MagicMock(NodestreamCommand)
    migrations = mocker.Mock(ProjectMigrations)
    subject = GenerateMigration(migrations, None, True)
    await subject.perform(command)
    migrations.detect_changes.assert_awaited_once()


def test_cleo_input_ask_yes_no(mocker):
    input = CleoMigrationInput(command := mocker.Mock(NodestreamCommand))
    command.confirm.return_value = True
    result = input.ask_yes_no("Some question?")
    assert_that(result, equal_to(True))
    command.confirm.assert_called_once_with("Some question?", default=False)
