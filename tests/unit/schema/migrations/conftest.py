import pytest


from nodestream.schema.migrations import Migration, MigrationGraph


@pytest.fixture
def leaf_migration(root_migration):
    return Migration(
        name="leaf_migration", operations=[], dependencies=[root_migration.name]
    )


@pytest.fixture
def root_migration():
    return Migration(name="root_migration", operations=[], dependencies=[])


@pytest.fixture()
def migration_graph(leaf_migration, root_migration):
    return MigrationGraph.from_iterable((leaf_migration, root_migration))
