from pathlib import Path
from typing import AsyncIterable, Optional, Tuple

from ..state import Schema
from .auto_change_detector import AutoChangeDetector, MigratorInput
from .auto_migration_maker import AutoMigrationMaker
from .migrations import Migration, MigrationGraph
from .migrator import Migrator
from .state_providers import MigrationGraphStateProvider, StaticStateProvider


class ProjectMigrations:
    """The migrations for a project.

    This class is used to be the interface between the project and the schema
    migrations system. It is used to execute migrations and generate migrations
    from changes in the project schema.
    """

    def __init__(self, graph: MigrationGraph, source_directory: Path) -> None:
        self.graph = graph
        self.source_directory = source_directory

    @classmethod
    def from_directory(cls, directory: Path) -> "ProjectMigrations":
        """Load migrations from a directory.

        Args:
            directory: The directory to load migrations from.

        Returns:
            The loaded migrations.
        """
        graph = MigrationGraph.from_directory(directory)
        return cls(graph, directory)

    async def determine_pending(
        self, migrator: Migrator
    ) -> AsyncIterable[Tuple[Migration, bool]]:
        """Determine pending migrations.

        This method determines the pending migrations for a migrator. A pending
        migration is a migration that has not been executed yet.

        Args:
            migrator: The migrator to use to determine pending migrations.

        Yields:
            Each migration and a boolean indicating if it is pending.
        """
        completed_migrations = await migrator.get_completed_migrations(self.graph)
        for migration in self.graph.topological_order():
            yield migration, migration not in completed_migrations

    async def execute_pending(self, migrator: Migrator) -> AsyncIterable[Migration]:
        """Execute pending migrations.

        This method executes all pending (non-completed) migrations.

        Args:
            migrator: The migrator to use to execute migrations.

        Yields:
            The names of the migrations that were executed.
        """
        async for migration, pending in self.determine_pending(migrator):
            if pending:
                await migrator.execute_migration(migration)
                yield migration

    async def detect_changes(
        self, input: MigratorInput, current_state: Schema
    ) -> Optional[Migration]:
        """Detect changes in the project schema.

        This method detects changes in the project schema compared to the
        current migration state. If there are no changes, then this method
        returns None.

        Args:
            input: The input interface for the migrator to ask questions.
            project_schema: The schema of the project in its current state.

        Returns:
            The generated migration, or None if there were no changes.
        """
        detector = AutoChangeDetector(
            input,
            MigrationGraphStateProvider(self.graph),
            StaticStateProvider(current_state),
        )
        maker = AutoMigrationMaker(self.graph, detector)
        return await maker.make_migration()

    async def create_migration_from_changes(
        self, input: MigratorInput, current_state: Schema
    ) -> Optional[Tuple[Migration, Path]]:
        """Create a migration from changes.

        Calls `detect_changes` and if there are changes, then it creates a
        migrtation file in the project and returns the migration and the path
        to the file. If there are no changes, then this method returns None.

        Args:
            input: The input interface for the migrator to ask questions.
            project_schema: The schema of the project in its current state.

        Returns:
            The generated migration and path to it, or None if there were no changes.
        """
        if (migration := await self.detect_changes(input, current_state)) is None:
            return None
        path = migration.write_to_file_with_default_name(self.source_directory)
        return migration, path

    def create_squash_between(
        self, from_migration: Migration, to_migration: Optional[Migration] = None
    ) -> Tuple[Migration, Path]:
        """Create a squashed migration between two migrations.

        Args:
            from_migration: The migration to squash from.
            to_migration: The migration to squash to.

        Returns:
            The squashed migration and the path to the file.
        """
        if to_migration is None:
            name = "squash_from_{}".format(from_migration.name)
        else:
            name = "squash_from_{}_to_{}".format(from_migration.name, to_migration.name)
        squashed = self.graph.squash_between(name, from_migration, to_migration)
        path = squashed.write_to_file_with_default_name(self.source_directory)
        return squashed, path
