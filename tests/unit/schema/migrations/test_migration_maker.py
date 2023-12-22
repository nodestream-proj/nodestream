import pytest
from freezegun import freeze_time
from hamcrest import assert_that, equal_to

from nodestream.schema.migrations.migrations import Migration
from nodestream.schema.migrations.migration_maker import AutoMigrationMaker


@pytest.fixture
def migration_graph(mocker):
    migration_graph = mocker.Mock()
    migration_graph.get_leaf_migrations.return_value = [
        Migration(name="a", operations=[], dependencies=[]),
        Migration(name="b", operations=[], dependencies=[]),
        Migration(name="c", operations=[], dependencies=[]),
    ]
    return migration_graph


@pytest.fixture
def auto_detector(mocker):
    auto_detector = mocker.AsyncMock()
    auto_detector.detect_changes.return_value = ["a", "b", "c"]
    return auto_detector


@pytest.fixture
def maker(migration_graph, auto_detector):
    return AutoMigrationMaker(migration_graph, auto_detector)


@freeze_time("2023-01-01 00:00:00")
def test_make_migration_name_many_operations(maker):
    result = maker.make_migration_name(["a", "b", "c"])
    assert_that(result, equal_to("20230101000000"))


@freeze_time("2023-01-01 00:00:00")
def test_make_migration_name_one_operation(maker, mocker):
    operation = mocker.Mock()
    operation.suggest_migration_name_slug.return_value = "a"
    result = maker.make_migration_name([operation])
    assert_that(result, equal_to("20230101000000_a"))


def test_make_migrations_dependency_names(maker):
    result = maker.get_migration_dependency_names()
    assert_that(result, equal_to(["a", "b", "c"]))


@freeze_time("2023-01-01 00:00:00")
@pytest.mark.asyncio
async def test_make_migration(maker):
    assert_that(
        await maker.make_migration(),
        equal_to(
            Migration(
                name="20230101000000",
                operations=["a", "b", "c"],
                dependencies=["a", "b", "c"],
            )
        ),
    )


@pytest.mark.asyncio
async def test_make_migration_no_changes(maker, auto_detector):
    auto_detector.detect_changes.return_value = []
    assert_that(await maker.make_migration(), equal_to(None))
