from contextlib import asynccontextmanager
from typing import List

from .migrations import Migration, MigrationGraph
from .operations import Operation


class Migrator:
    """Abstract class for executing migrations.

    This class is used by the schema change detector to execute migrations
    after they have been detected.
    """

    async def acquire_lock(self) -> None:
        """Acquire a lock against the database to prevent concurrent ops.

        This method should block until the lock is acquired. If the lock cannot
        be acquired, then this method should raise an exception.
        """
        pass

    async def release_lock(self) -> None:
        """Release the lock against the database.

        This method should release the lock against the database. If the lock
        cannot be released, then this method should raise an exception.
        """
        pass

    async def start_transaction(self) -> None:
        """Start a transaction.

        For databases that do not support transactions, this method should be a
        no-op. For databases that do support transactions, this method should
        start a transaction. If the transaction cannot be started, then this
        method should raise an exception.
        """
        pass

    async def commit_transaction(self) -> None:
        """Commit a transaction.

        For databases that do not support transactions, this method should be a
        no-op. For databases that do support transactions, this method should
        commit the transaction. If the transaction cannot be committed, then
        this method should raise an exception.

        This method is only called if start_transaction() was called and
        did not raise an exception.
        """
        pass

    async def rollback_transaction(self) -> None:
        """Rollback a transaction.

        For databases that do not support transactions, this method should be a
        no-op. For databases that do support transactions, this method should
        rollback the transaction. If the transaction cannot be rolled back,
        then this method should raise an exception.

        This method is only called if start_transaction() was called and did
        not raise an exception.
        """
        pass

    async def execute_operation(self, operation: Operation) -> None:
        """Execute an operation.

        Takes an operation and executes it against the database. If the
        operation cannot be executed, then this method should raise an
        exception. If the operation is executed, then this method should
        return None.

        This method is only called if start_transaction() was called and did
        not raise an exception.

        Args:
            operation: The operation to execute.
        """
        pass

    async def mark_migration_as_executed(self, migration: Migration) -> None:
        """Mark a migration as executed.

        This method should mark a migration as executed. If the migration
        cannot be marked as executed, then this method should raise an
        exception.

        This method is only called if start_transaction() was called and did
        not raise an exception.

        By default, this method does nothing. This method should be overridden
        by subclasses to mark a migration as executed assuming that the
        migrations are stateful.

        Args:
            migration: The migration to mark as executed.
        """
        pass

    async def get_completed_migrations(self, graph: MigrationGraph) -> List[Migration]:
        """Get a list of completed migrations.

        This method should return a list of completed migrations. If the
        migrations cannot be retrieved, then this method should raise an
        exception.

        By default, this method returns an empty list. This method should be
        overridden by subclasses to return a list of completed migrations
        assuming that the migrations are stateful.

        Returns:
            A list of completed migrations.
        """
        return []

    @asynccontextmanager
    async def transaction(self):
        await self.start_transaction()
        try:
            yield
            await self.commit_transaction()
        except Exception as e:
            await self.rollback_transaction()
            raise e

    async def execute_migration(self, migration: Migration) -> None:
        """Execute a migration.

        Args:
            migration: The migration to execute.
        """
        async with self.transaction():
            await self.acquire_lock()

        async with self.transaction():
            try:
                for operation in migration.operations:
                    await self.execute_operation(operation)
            except Exception:
                await self.release_lock()
                raise

        async with self.transaction():
            await self.mark_migration_as_executed(migration)

        async with self.transaction():
            await self.release_lock()


class OperationTypeNotSupportedError(Exception):
    """Raised when an operation type is not supported."""

    pass


class OperationTypeRoutingMixin:
    """Mixin for routing operations to the correct method.

    This mixin is used for routing operations to the correct method. It provides
    a default implementation of execute_operation() that routes the operation
    to the correct method based on the operation type.

    The method that an operation is routed to is determined by converting the
    operation type to snake case and prepending it with "execute_". For
    example, the operation type "CreateNodeType" would be routed to the method
    "execute_create_node_type()".

    If the operation type cannot be routed to a method, then an
    OperationTypeNotSupportedError is raised.

    The method that an operation is routed to must be implemented by the class
    that uses this mixin.
    """

    async def execute_operation(self, operation: Operation) -> None:
        type_as_snake = operation.type_as_snake_case()
        method_name = f"execute_{type_as_snake}"
        try:
            method = getattr(self, method_name)
            await method(operation)
        except AttributeError:
            raise OperationTypeNotSupportedError(
                f"Unable to route operation to method: {method_name}"
            )
