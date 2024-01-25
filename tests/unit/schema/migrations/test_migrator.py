import pytest
from hamcrest import assert_that, equal_to

from nodestream.schema.migrations.migrator import (
    Migrator,
    OperationTypeRoutingMixin,
    OperationTypeNotSupportedError,
)
from nodestream.schema.migrations.operations import CreateNodeType


@pytest.fixture
def migrator(mocker):
    migrator = Migrator()
    migrator.start_transaction = mocker.AsyncMock()
    migrator.commit_transaction = mocker.AsyncMock()
    migrator.rollback_transaction = mocker.AsyncMock()
    migrator.acquire_lock = mocker.AsyncMock()
    migrator.release_lock = mocker.AsyncMock()
    migrator.execute_operation = mocker.AsyncMock()
    migrator.mark_migration_as_executed = mocker.AsyncMock()
    return migrator


@pytest.fixture
def migration_with_work(root_migration):
    root_migration.operations.append(CreateNodeType("Foo", ["name"], []))
    return root_migration


@pytest.mark.asyncio
async def test_default_get_completed_migrations(migration_graph):
    migrator = Migrator()
    result = await migrator.get_completed_migrations(migration_graph)
    assert_that(result, equal_to([]))


@pytest.mark.asyncio
async def test_execute_migration_happy_path(migrator, migration_with_work):
    await migrator.execute_migration(migration_with_work)
    assert_that(migrator.start_transaction.await_count, equal_to(4))
    assert_that(migrator.commit_transaction.await_count, equal_to(4))
    migrator.rollback_transaction.assert_not_called()
    migrator.acquire_lock.assert_awaited_once()
    migrator.release_lock.assert_awaited_once()


@pytest.mark.asyncio
async def test_execute_migration_fail_to_acquire_lock(migrator, migration_with_work):
    migrator.acquire_lock.side_effect = RuntimeError()
    with pytest.raises(RuntimeError):
        await migrator.execute_migration(migration_with_work)
    assert_that(migrator.start_transaction.await_count, equal_to(1))
    assert_that(migrator.commit_transaction.await_count, equal_to(0))
    migrator.rollback_transaction.assert_awaited_once()
    migrator.acquire_lock.assert_awaited_once()
    migrator.release_lock.assert_not_called()


@pytest.mark.asyncio
async def test_execute_migration_fail_to_execute_operation(
    migrator, migration_with_work
):
    migrator.execute_operation.side_effect = RuntimeError()
    with pytest.raises(RuntimeError):
        await migrator.execute_migration(migration_with_work)
    assert_that(migrator.start_transaction.await_count, equal_to(2))
    assert_that(migrator.commit_transaction.await_count, equal_to(1))
    migrator.rollback_transaction.assert_awaited_once()
    migrator.acquire_lock.assert_awaited_once()
    migrator.release_lock.assert_not_called()


@pytest.mark.asyncio
async def test_execute_migration_fail_to_mark_migration_as_executed(
    migrator, migration_with_work
):
    migrator.mark_migration_as_executed.side_effect = RuntimeError()
    with pytest.raises(RuntimeError):
        await migrator.execute_migration(migration_with_work)
    assert_that(migrator.start_transaction.await_count, equal_to(3))
    assert_that(migrator.commit_transaction.await_count, equal_to(2))
    migrator.rollback_transaction.assert_awaited_once()
    migrator.acquire_lock.assert_awaited_once()
    migrator.release_lock.assert_not_called()


@pytest.mark.asyncio
async def test_execute_migration_fail_to_release_lock(migrator, migration_with_work):
    migrator.release_lock.side_effect = RuntimeError()
    with pytest.raises(RuntimeError):
        await migrator.execute_migration(migration_with_work)
    assert_that(migrator.start_transaction.await_count, equal_to(4))
    assert_that(migrator.commit_transaction.await_count, equal_to(3))
    migrator.rollback_transaction.assert_awaited_once()
    migrator.acquire_lock.assert_awaited_once()
    migrator.release_lock.assert_awaited_once()


@pytest.mark.asyncio
async def test_operation_routing_happy_path(mocker):
    type_router = OperationTypeRoutingMixin()
    type_router.execute_create_node_type = mocker.AsyncMock()
    operation = CreateNodeType("Foo", ["name"], [])
    await type_router.execute_operation(operation)
    type_router.execute_create_node_type.assert_awaited_once_with(operation)


@pytest.mark.asyncio
async def test_operation_routing_missing_handler():
    type_router = OperationTypeRoutingMixin()
    operation = CreateNodeType("Foo", ["name"], [])
    with pytest.raises(OperationTypeNotSupportedError):
        await type_router.execute_operation(operation)
