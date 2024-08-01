from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional

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
    replaces: List[str] = field(default_factory=list)

    def to_file_data(self):
        return {
            "name": self.name,
            "operations": [operation.to_file_data() for operation in self.operations],
            "dependencies": self.dependencies,
            "replaces": self.replaces,
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
            replaces=file_data.get("replaces", []),
        )

    @classmethod
    def describe_yaml_schema(cls):
        from schema import Optional, Schema

        return Schema(
            {
                "name": str,
                "operations": [Operation.describe_yaml_schema()],
                "dependencies": [str],
                Optional("replaces"): [str],
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

    def is_squashed_migration(self) -> bool:
        """Check if this migration is a squashed migration.

        Returns:
            True if this migration is a squashed migration, False otherwise.
        """
        return len(self.replaces) > 0

    @classmethod
    def squash(
        cls,
        new_name: str,
        migrations: Iterable["Migration"],
        optimize_operations: bool = True,
    ) -> "Migration":
        """Make a new migration as the squashed for the given migrations.

        Args:
            new_name: The name of the new migration.
            migrations: The migrations to squash.
            optimize_operations: Whether to optimize the operations
            before squashing.

        Returns:
            The new migration that is the effective squash of the
            given migrations.
        """
        names_of_migrations_being_squashed = {m.name for m in migrations}
        effective_operations = [o for m in migrations for o in m.operations]
        dependencies = list(
            {
                d
                for m in migrations
                for d in m.dependencies
                if d not in names_of_migrations_being_squashed
            }
        )
        if optimize_operations:
            effective_operations = Operation.optimize(effective_operations)

        return cls(
            name=new_name,
            operations=effective_operations,
            replaces=list(names_of_migrations_being_squashed),
            dependencies=dependencies,
        )


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

    def get_ordered_migration_plan(
        self, completed_migrations: List[Migration]
    ) -> List[Migration]:
        completed_migration_names = {m.name for m in completed_migrations}
        replacement_index = {r: m for m in self.all_migrations() for r in m.replaces}
        plan_order = []

        for migration in self.topological_order():
            if migration.name in completed_migration_names:
                continue

            # If we are considering a migration that has been replaced,
            # we _only_ want to add it if at least one of the migrations
            # replaced by the replacing migration has not been completed.
            # IN otherwords, if the changes from a squashed migration have
            # have (at least partially) been applied, we don't want to add
            # the squashed migration to the plan but will want to add the
            # migrations that were replaced to the plan.
            if (replacement := replacement_index.get(migration.name)) is not None:
                if not any(
                    r in completed_migration_names for r in replacement.replaces
                ):
                    continue

            # Similarly, if we are looking at a squashed migration, we want to
            # add it to the plan only if none of the migrations that it replaces
            # have been completed.
            if migration.is_squashed_migration():
                if any(r in completed_migration_names for r in migration.replaces):
                    continue

            # Now here we are in one of three stats:
            #
            # 1. The migraiton is some "regular" migration that has not been
            #       completed.
            # 2. The migration is a squashed migration that we want to add to
            #       the plan.
            # 3. The migration is a migration that has been replaced by a
            #       squashed migration but we've determined that at least one
            #       of the migrations that it replaces has not been completed.
            #
            # In all of these cases, we want to add the migration to the plan.
            plan_order.append(migration)

        return plan_order

    def _iterative_dfs_traversal(self, *start_node: Migration) -> List[Migration]:
        visited_order = []
        visited_set = set()
        stack = [(n, False) for n in start_node]

        while stack:
            node, processed = stack.pop()
            if node.name in visited_set:
                continue
            if processed:
                visited_order.append(node)
                visited_set.add(node.name)
                continue

            stack.append((node, True))
            stack.extend(
                ((self.migrations_by_name[n], False) for n in node.dependencies)
            )

        return visited_order

    def squash_between(
        self,
        name: str,
        from_migration: Migration,
        to_migration: Optional[Migration] = None,
    ):
        """Squash all migrations between two migrations.

        Args:
            name: The name of the new squashed migration.
            from_migration: The migration to start squashing from.
            to_migration: The migration to stop squashing at.

        Returns:
            The new squashed migration.
        """
        ordered = self.topological_order()
        from_index = ordered.index(from_migration)
        to_index = ordered.index(to_migration) if to_migration else len(ordered) - 1
        migrations_to_squash = ordered[from_index : to_index + 1]
        return Migration.squash(name, migrations_to_squash)

    def topological_order(self):
        return self._iterative_dfs_traversal(*self.get_leaf_migrations())

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
