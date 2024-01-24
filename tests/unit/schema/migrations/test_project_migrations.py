import pytest
from hamcrest import assert_that, equal_to, has_length

from nodestream.schema.migrations import Migrator, ProjectMigrations, MigratorInput


@pytest.fixture
def subject(migration_graph, tmp_path):
    return ProjectMigrations(migration_graph, tmp_path)


@pytest.mark.asyncio
async def test_determine_pending(subject, mocker, root_migration, leaf_migration):
    migrator = mocker.AsyncMock(Migrator)
    migrator.get_completed_migrations.return_value = [root_migration]
    results = [r async for r in subject.determine_pending(migrator)]
    assert_that(results, equal_to([(root_migration, False), (leaf_migration, True)]))


@pytest.mark.asyncio
async def test_execute_pending(subject, mocker, root_migration, leaf_migration):
    migrator = mocker.AsyncMock(Migrator)
    migrator.get_completed_migrations.return_value = [root_migration]
    results = [r async for r in subject.execute_pending(migrator)]
    assert_that(results, equal_to([leaf_migration]))
    migrator.execute_migration.assert_awaited_once_with(leaf_migration)


@pytest.mark.asyncio
async def test_create_migration_from_changes(subject, mocker, leaf_migration):
    subject.detect_changes = mocker.AsyncMock(return_value=leaf_migration)
    migration, path = await subject.create_migration_from_changes(None, None)
    assert_that(migration, equal_to(leaf_migration))
    assert_that(path.parent, equal_to(subject.source_directory))


@pytest.mark.asyncio
async def test_detect_changes(subject, basic_schema):
    # The migration graph does not do anything to the schema, therefore,
    # all the changes in basic_schema should be operations. This is a bit more of an
    # integration test than a unit test, but I would argue is more meaningful than
    # mocking out a bunch of stuff.
    result = await subject.detect_changes(MigratorInput(), basic_schema)
    assert_that(result.operations, has_length(4))
