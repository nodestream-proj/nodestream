from datetime import datetime
from typing import List, Optional

from .auto_change_detector import AutoChangeDetector
from .migrations import Migration, MigrationGraph
from .operations import Operation


class AutoMigrationMaker:
    """A class that can make migrations automatically.

    This class is responsible for detecting changes to the schema and
    automatically generating migrations from those changes.
    """

    __slots__ = ("migration_graph", "auto_detector")

    def __init__(
        self,
        migration_graph: MigrationGraph,
        auto_detector: AutoChangeDetector,
    ) -> None:
        """Initialize the migration maker.

        Args:
            migration_graph: The migration graph to use.
            auto_detector: The auto detector of schema changes.
        """

        self.migration_graph = migration_graph
        self.auto_detector = auto_detector

    def make_migration_name(self, operations: List[Operation]) -> str:
        """Make a migration name from a list of operations.

        If the list of operations contains only one operation, then the
        migration name will be the timestamp followed by the slug of the
        operation. Otherwise, the migration name will be the timestamp.

        Args:
            operations: A list of operations.

        Returns:
            A migration name.
        """
        base_name = datetime.now().strftime("%Y%m%d%H%M%S")
        if len(operations) == 1:
            return f"{base_name}_{operations[0].suggest_migration_name_slug()}"

        return base_name

    def get_migration_dependency_names(self) -> List[str]:
        """Get the names of the migrations that this migration depends on.

        Logically, we are building a new migration that depends on the
        current state of the database (all migrations thus far) as well as
        the current state of the schema (from all pipelines).

        Therefore, we need this migration to depend on all leaf nodes in the
        migration graph. Which is really all that this method does.

        Returns:
            A list of migration names.
        """
        return [m.name for m in self.migration_graph.get_leaf_migrations()]

    async def make_migration(self) -> Optional[Migration]:
        """Make a migration from the changes that were detected.

        Returns:
            A migration describing the changes that were detected. If no
            changes were detected, then this method returns None.
        """
        operations = await self.auto_detector.detect_changes()
        if len(operations) == 0:
            return None

        return Migration(
            name=self.make_migration_name(operations),
            operations=operations,
            dependencies=self.get_migration_dependency_names(),
        )
