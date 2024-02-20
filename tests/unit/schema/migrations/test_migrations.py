import pytest
from hamcrest import assert_that, equal_to, is_

from nodestream.schema.migrations import Migration, MigrationGraph
from nodestream.schema.migrations.operations import CreateNodeType


def test_migration_is_root_migration(root_migration):
    assert_that(root_migration.is_root_migration(), is_(equal_to(True)))


def test_migration_is_not_root_migration(leaf_migration):
    assert_that(leaf_migration.is_root_migration(), is_(equal_to(False)))


def test_migration_is_leaf_migration(leaf_migration, migration_graph):
    assert_that(leaf_migration.is_leaf_migration(migration_graph), is_(equal_to(True)))


def test_migration_is_not_leaf_migration(migration_graph, root_migration):
    assert_that(root_migration.is_leaf_migration(migration_graph), is_(equal_to(False)))


def test_migration_write_to_file_with_default_name(tmp_path, root_migration):
    root_migration.write_to_file_with_default_name(tmp_path)
    assert_that((tmp_path / "root_migration.yaml").exists(), is_(equal_to(True)))


def test_migration_to_and_from_file_data():
    migration = Migration(
        name="test_migration",
        operations=[CreateNodeType("Person", {"name"}, {"age"})],
        dependencies=["other_migration"],
    )
    file_data = migration.to_file_data()
    assert_that(Migration.validate_and_load(file_data), is_(equal_to(migration)))


@pytest.mark.parametrize(
    "migration_graph",
    [
        MigrationGraph.from_iterable(
            [
                Migration(name="a", operations=[], dependencies=[]),
                Migration(name="b", operations=[], dependencies=["a"]),
                Migration(name="c", operations=[], dependencies=["b"]),
            ]
        ),
        MigrationGraph.from_iterable(
            [
                Migration(name="a", operations=[], dependencies=[]),
                Migration(name="b", operations=[], dependencies=["a"]),
                Migration(name="c", operations=[], dependencies=["b"]),
                Migration(name="d", operations=[], dependencies=["b"]),
            ]
        ),
        MigrationGraph.from_iterable(
            [
                Migration(name="b", operations=[], dependencies=["a"]),
                Migration(name="a", operations=[], dependencies=[]),
                Migration(name="c", operations=[], dependencies=["b"]),
                Migration(name="e", operations=[], dependencies=["c", "d"]),
                Migration(name="d", operations=[], dependencies=["b"]),
            ]
        ),
    ],
)
def test_migration_graph_test_get_ordered_migration_plan(migration_graph):
    plan = migration_graph.get_ordered_migration_plan()
    assert_that(len(plan), is_(equal_to(len(migration_graph.migrations_by_name))))
    for i, migration in enumerate(plan):
        migrations_to_current = {m.name for m in plan[0:i]}
        assert_that(
            all(
                dependency in migrations_to_current
                for dependency in migration.dependencies
            ),
            is_(equal_to(True)),
        )


def test_migration_graph_get_leaf_migrations():
    migration_graph = MigrationGraph.from_iterable(
        [
            Migration(name="a", operations=[], dependencies=[]),
            Migration(name="b", operations=[], dependencies=["a"]),
            Migration(name="c", operations=[], dependencies=["b"]),
            Migration(name="d", operations=[], dependencies=["b"]),
            Migration(name="e", operations=[], dependencies=["c", "d"]),
            Migration(name="f", operations=[], dependencies=["d"]),
        ]
    )
    leaf_migrations = migration_graph.get_leaf_migrations()
    assert_that({m.name for m in leaf_migrations}, is_(equal_to({"e", "f"})))


def test_migration_graph_from_directory(tmp_path):
    migration = Migration(
        name="test_migration",
        operations=[CreateNodeType("Person", {"name"}, {"age"})],
        dependencies=["other_migration"],
    )
    migration.write_to_file_with_default_name(tmp_path)
    migration_graph = MigrationGraph.from_directory(tmp_path)
    assert_that(
        migration_graph, is_(equal_to(MigrationGraph.from_iterable([migration])))
    )


def test_migration_graph_get_by_name(migration_graph, root_migration):
    result = migration_graph.get_migration("root_migration")
    assert_that(result, equal_to(root_migration))
