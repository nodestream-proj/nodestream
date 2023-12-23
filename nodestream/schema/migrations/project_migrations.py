from pathlib import Path
from typing import Optional, AsyncIterable

from .auto_migration_maker import AutoMigrationMaker
from .auto_change_detector import AutoChangeDetector, MigratorInput
from .migrations import MigrationGraph
from .migrator import Migrator
from .state_providers import MigrationGraphStateProvider, StaticStateProvider
from ..state import Schema


class ProjectMigrations:
    """The migrations for a project.

    This class is used to be the interface between the project and the schema
    migrations system. It is used to execute migrations and generate migrations
    from changes in the project schema.
    """

    __slots__ = ("graph", "source_directory")

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

    async def execute_pending(self, migrator: Migrator) -> AsyncIterable[str]:
        """Execute pending migrations.

        This method executes all pending (non-completed) migrations.

        Args:
            migrator: The migrator to use to execute migrations.

        Yields:
            The names of the migrations that were executed.
        """
        completed_migrations = await migrator.get_completed_migrations()
        for migration in self.graph.get_ordered_migration_plan():
            if migration not in completed_migrations:
                await migrator.execute_migration(migration)
                yield migration.name

    async def generate_migration_from_changes(
        self, input: MigratorInput, project_schema: Schema
    ) -> Optional[Path]:
        """Generate a migration from changes.

        This method generates a migration from changes in the project schema
        compared to the current migration state. If there are no changes, then
        this method returns None.

        Args:
            input: The input interface for the migrator to ask questions.
            project_schema: The schema of the project in its current state.

        Returns:
            The path to the generated migration, or None if there are no
            changes.
        """

        detector = AutoChangeDetector(
            input,
            MigrationGraphStateProvider(self.graph),
            StaticStateProvider(project_schema),
        )
        maker = AutoMigrationMaker(self.graph, detector)
        migration = await maker.make_migration()
        if not migration:
            return None
        return migration.write_to_file_with_default_name(self.source_directory)
