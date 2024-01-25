import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.commands import ShowMigrations
from nodestream.project import Target
from nodestream.schema.migrations import ProjectMigrations


@pytest.mark.asyncio
async def test_handle_async(mocker):
    show = ShowMigrations()
    header = ["migration", "opeations", "t1"]
    rows = [["1", "12", "good"]]
    show.generate_output_table = mocker.AsyncMock(return_value=(header, rows))
    show.table = mocker.Mock()
    await show.handle_async()
    show.table.assert_called_once_with(header, rows)
    show.table.return_value.render.assert_called_once()


@pytest.mark.asyncio
async def test_generate_output_table(mocker, project_with_default_scope, basic_schema):
    show = ShowMigrations()
    show.get_target_names = mocker.Mock(return_value={"t1", "t2"})
    migration_status_by_target = {
        "m2": {
            "t2": True,
            "t1": False,
        },
        "m1": {
            "t2": False,
            "t1": False,
        },
    }
    show.get_project = mocker.Mock(return_value=project_with_default_scope)
    show.get_migrations = mocker.Mock()
    project_with_default_scope.get_schema = mocker.Mock(return_value=basic_schema)
    show.get_migration_status_by_target = mocker.AsyncMock(
        return_value=migration_status_by_target
    )
    actual_headers, actual_rows = await show.generate_output_table()
    expected_headers = ["Migration", "t1", "t2"]
    expected_rows = [
        ["m1", "✅", "✅"],
        ["m2", "✅", "❌"],
    ]
    assert_that(actual_rows, equal_to(expected_rows))
    assert_that(actual_headers, equal_to(expected_headers))


def test_specify_targets_cli_unspecified(mocker, project_with_default_scope):
    show = ShowMigrations()
    show.option = mocker.Mock(return_value=[])
    project_with_default_scope.targets_by_name = {"t1": "t1"}
    result = show.get_target_names(project_with_default_scope)
    assert_that(list(result), equal_to(["t1"]))


def test_specify_targets_cli_specified(mocker, project_with_default_scope):
    show = ShowMigrations()
    show.option = mocker.Mock(return_value=["t2"])
    project_with_default_scope.targets_by_name = {"t1": "t1", "t2": "t2"}
    result = show.get_target_names(project_with_default_scope)
    assert_that(list(result), equal_to(["t2"]))


@pytest.mark.asyncio
async def test_get_migration_status_by_target(
    project_with_default_scope, migration_graph, project_dir
):
    t1 = Target("t1", {"database": "null"})
    t2 = Target("t2", {"database": "null"})
    project_with_default_scope.targets_by_name["t1"] = t1
    project_with_default_scope.targets_by_name["t2"] = t2
    migrations = ProjectMigrations(migration_graph, project_dir)
    show = ShowMigrations()
    result = await show.get_migration_status_by_target(
        project_with_default_scope, ["t1", "t2"], migrations
    )
    assert_that(
        result,
        equal_to(
            {
                "root_migration": {"t1": True, "t2": True},
                "leaf_migration": {"t1": True, "t2": True},
            }
        ),
    )
