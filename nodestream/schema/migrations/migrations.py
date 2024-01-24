from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List

from ...file_io import LoadsFromYamlFile, SavesToYamlFile
from .operations import Operation


@dataclass(frozen=True, slots=True)
class Migration(LoadsFromYamlFile, SavesToYamlFile):
    """A migration is a collection of operations that can be applied to a schema.

    Migrations are used to migrate the database schema from one version to another.
    """

    name: str
    operations: List[Operation]
    dependencies: List[str]

    def to_file_data(self):
        return {
            "name": self.name,
            "operations": [operation.to_file_data() for operation in self.operations],
            "dependencies": self.dependencies,
        }

    @classmethod
    def from_file_data(cls, file_data):
        return cls(
            name=file_data["name"],
            operations=[
                Operation.from_file_data(operation_file_data)
                for operation_file_data in file_data["operations"]
            ],
            dependencies=file_data["dependencies"],
        )

    @classmethod
    def describe_yaml_schema(cls):
        from schema import Schema

        return Schema(
            {
                "name": str,
                "operations": [Operation.describe_yaml_schema()],
                "dependencies": [str],
            }
        )

    def is_root_migration(self) -> bool:
        """Check if this migration is a root migration.

        Returns:
            True if this migration is a root migration, False otherwise.
        """
        return len(self.dependencies) == 0

    def is_leaf_migration(self, graph: "MigrationGraph") -> bool:
        """Check if this migration is a leaf migration.

        Returns:
            True if this migration is a leaf migration, False otherwise.
        """
        dependents = (m for m in graph.all_migrations() if self.name in m.dependencies)
        return not any(dependents)

    def write_to_file_with_default_name(self, directory: Path) -> Path:
        """Save the migration to a directory with a default name.

        Args:
            directory: The directory to save the migration to.
        """
        path = directory / f"{self.name}.yaml"
        self.write_to_file(path)
        return path


@dataclass(frozen=True, slots=True)
class MigrationGraph:
    """A directed acyclic graph of migrations.

    The graph is represented as an adjacency list.
    """

    migrations_by_name: Dict[str, Migration]

    def get_migration(self, name: str) -> Migration:
        """Get a migration by name.

        Args:
            name: The name of the migration to get.

        Returns:
            The migration with the given name.

        Raises:
            KeyError: If there is no migration with the given name.
        """
        return self.migrations_by_name[name]

    def get_ordered_migration_plan(self) -> List[Migration]:
        plan_order = []

        for migration in self.all_migrations():
            for required_migration in self.plan_to_execute_migration(migration):
                if required_migration not in plan_order:
                    plan_order.append(required_migration)

        return plan_order

    def _iterative_dfs_traversal(self, start_node: Migration) -> List[Migration]:
        visited_order = []
        visited_set = set()
        stack = [(start_node, False)]

        while stack:
            node, proccessed = stack.pop()
            if node.name in visited_set:
                continue
            if proccessed:
                visited_order.append(node)
                visited_set.add(node.name)
                continue

            stack.append((node, True))
            stack.extend(
                ((self.migrations_by_name[n], False) for n in node.dependencies)
            )

        return visited_order

    def plan_to_execute_migration(self, migration: Migration) -> List[Migration]:
        return self._iterative_dfs_traversal(migration)

    def all_migrations(self) -> Iterable[Migration]:
        """Iterate over all migrations in the graph."""
        return self.migrations_by_name.values()

    def get_leaf_migrations(self) -> Iterable[Migration]:
        """Iterate over all leaf migrations in the graph."""
        return [
            migration
            for migration in self.all_migrations()
            if migration.is_leaf_migration(self)
        ]

    @classmethod
    def from_directory(cls, directory: Path) -> "MigrationGraph":
        """Load a migration graph from a directory."""

        migrations = (
            Migration.read_from_file(migration_file)
            for migration_file in directory.glob("*.yaml")
        )
        return cls.from_iterable(migrations)

    @classmethod
    def from_iterable(cls, migrations: Iterable[Migration]) -> "MigrationGraph":
        """Load a migration graph from a list of migrations."""
        return cls(migrations_by_name={m.name: m for m in migrations})
