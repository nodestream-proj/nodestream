import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.commands.copy import Copy, UnknownTargetError
from nodestream.cli.operations import RunCopy
from nodestream.project import Project, Target


@pytest.fixture
def copy_command():
    return Copy()


@pytest.fixture
def project():
    return Project(
        [],
        {},
        {
            "test": Target("test", {"uri": "bolt://localhost:7687"}),
            "test2": Target("test2", {"uri": "bolt://localhost:7687"}),
        },
    )


def test_get_target_from_user_from_option(copy_command, project, mocker):
    copy_command.option = mocker.Mock(return_value="test")
    target = copy_command.get_taget_from_user(project, "from")
    assert_that(target, equal_to(project.targets_by_name["test"]))
    copy_command.option.assert_called_once_with("from")


def test_get_target_from_user_from_prompt(copy_command, project, mocker):
    copy_command.option = mocker.Mock(return_value=None)
    copy_command.choice = mocker.Mock(return_value="test")
    target = copy_command.get_taget_from_user(project, "from")
    assert_that(target, equal_to(project.targets_by_name["test"]))
    copy_command.option.assert_called_once_with("from")
    copy_command.choice.assert_called_once_with(
        "Which target would you like to copy from?",
        ["test", "test2"],
    )


def test_get_target_from_user_from_option_unknown_target(copy_command, project, mocker):
    copy_command.line_error = mocker.Mock()
    with pytest.raises(UnknownTargetError):
        copy_command.option = mocker.Mock(return_value="unknown")
        copy_command.get_taget_from_user(project, "from")
    copy_command.line_error.assert_called_once_with("Unknown target: unknown")


# ---------------------------------------------------------------------------
# apply_schema_filter tests
# ---------------------------------------------------------------------------


def test_apply_schema_filter_no_filters_returns_full_schema(
    copy_command, mocker, basic_schema
):
    copy_command.option = mocker.Mock(
        side_effect=lambda name: [] if name in ("node", "relationship") else None
    )
    result = copy_command.apply_schema_filter(basic_schema)
    assert {n.name for n in result.nodes} == {n.name for n in basic_schema.nodes}


def test_apply_schema_filter_node_filter(copy_command, mocker, basic_schema):
    def opt(name):
        if name == "node":
            return ["Person"]
        return []

    copy_command.option = mocker.Mock(side_effect=opt)
    result = copy_command.apply_schema_filter(basic_schema)
    node_names = {n.name for n in result.nodes}
    assert "Person" in node_names


def test_apply_schema_filter_unknown_node_skipped(copy_command, mocker, basic_schema):
    def opt(name):
        if name == "node":
            return ["UnknownType"]
        return []

    copy_command.option = mocker.Mock(side_effect=opt)
    # Should not raise — unknown types are warned and skipped.
    result = copy_command.apply_schema_filter(basic_schema)
    # No unknown nodes survive into the filtered schema.
    node_names = {n.name for n in result.nodes}
    assert "UnknownType" not in node_names


def test_apply_schema_filter_unknown_relationship_skipped(
    copy_command, mocker, basic_schema
):
    def opt(name):
        if name == "relationship":
            return ["NONEXISTENT"]
        return []

    copy_command.option = mocker.Mock(side_effect=opt)
    # Should not raise — unknown types are warned and skipped.
    result = copy_command.apply_schema_filter(basic_schema)
    rel_names = {r.name for r in result.relationships}
    assert "NONEXISTENT" not in rel_names


def test_apply_schema_filter_known_relationship_included(
    copy_command, mocker, basic_schema
):
    def opt(name):
        if name == "relationship":
            return ["BEST_FRIEND_OF"]
        return []

    copy_command.option = mocker.Mock(side_effect=opt)
    result = copy_command.apply_schema_filter(basic_schema)
    rel_names = {r.name for r in result.relationships}
    assert "BEST_FRIEND_OF" in rel_names
    assert "HAS_EMPLOYEE" not in rel_names


# ---------------------------------------------------------------------------
# handle_async integration tests
# ---------------------------------------------------------------------------


def _make_handle_async_setup(
    copy_command, mocker, project, basic_schema, option_values
):
    project.make_schema_for_copy = mocker.Mock(return_value=basic_schema)
    copy_command.run_operation = mocker.AsyncMock(
        side_effect=[None, None, project, None]
    )
    copy_command.get_taget_from_user = mocker.Mock(
        side_effect=[project.targets_by_name["test"], project.targets_by_name["test2"]]
    )
    copy_command.apply_schema_filter = mocker.Mock(return_value=basic_schema)
    copy_command.option = mocker.Mock(side_effect=lambda name: option_values[name])
    mocker.patch.object(
        type(copy_command),
        "has_json_logging_set",
        new_callable=mocker.PropertyMock,
        return_value=False,
    )


_DEFAULT_OPTIONS = {
    "node": [],
    "relationship": [],
    "concurrency-limit": "1",
    "batch-size": "1000",
    "step-outbox-size": "10000",
    "flush-concurrency": "1",
    "connector-option": [],
    "retriever-option": [],
    "shard-size": None,
    "node-only": False,
    "reporting-frequency": "1000",
    "metrics-interval-in-seconds": None,
}


@pytest.mark.asyncio
async def test_handle_async_unknown_target_error(copy_command, mocker):
    copy_command.line_error = mocker.Mock()
    copy_command.run_operation = mocker.AsyncMock()
    copy_command.get_taget_from_user = mocker.Mock(side_effect=UnknownTargetError)
    result = await copy_command.handle_async()
    assert_that(result, equal_to(1))
    assert copy_command.run_operation.await_count == 3
    run_copy_calls = [
        c
        for c in copy_command.run_operation.call_args_list
        if isinstance(c.args[0], RunCopy)
    ]
    assert len(run_copy_calls) == 0


@pytest.mark.asyncio
async def test_handle_async(copy_command, mocker, basic_schema, project):
    _make_handle_async_setup(
        copy_command, mocker, project, basic_schema, _DEFAULT_OPTIONS
    )
    await copy_command.handle_async()
    assert copy_command.run_operation.await_count == 4

    run_copy_op = copy_command.run_operation.call_args_list[-1].args[0]
    assert isinstance(run_copy_op, RunCopy)
    assert run_copy_op.from_target == project.targets_by_name["test"]
    assert run_copy_op.to_target == project.targets_by_name["test2"]
    assert run_copy_op.schema is basic_schema
    assert run_copy_op.batch_size == 1000
    assert run_copy_op.flush_concurrency == 1
    assert run_copy_op.connector_overrides == {}
    assert run_copy_op.concurrency_limit == 1


@pytest.mark.asyncio
async def test_handle_async_with_non_default_options(
    copy_command, mocker, basic_schema, project
):
    opts = {
        **_DEFAULT_OPTIONS,
        "concurrency-limit": "4",
        "flush-concurrency": "3",
        "connector-option": ["uri=bolt://remote:7687"],
        "retriever-option": ["limit=500", "sample_ratio=50"],
    }
    _make_handle_async_setup(copy_command, mocker, project, basic_schema, opts)
    await copy_command.handle_async()

    run_copy_op = copy_command.run_operation.call_args_list[-1].args[0]
    assert isinstance(run_copy_op, RunCopy)
    assert run_copy_op.flush_concurrency == 3
    assert run_copy_op.connector_overrides == {"uri": "bolt://remote:7687"}
    assert run_copy_op.concurrency_limit == 4
    assert run_copy_op.retriever_overrides.get("limit") == 500
    assert run_copy_op.retriever_overrides.get("sample_ratio") == 50


@pytest.mark.asyncio
async def test_handle_async_with_shard_size(
    copy_command, mocker, basic_schema, project
):
    opts = {**_DEFAULT_OPTIONS, "shard-size": "5000"}
    _make_handle_async_setup(copy_command, mocker, project, basic_schema, opts)
    await copy_command.handle_async()
    run_copy_op = copy_command.run_operation.call_args_list[-1].args[0]
    assert run_copy_op.retriever_overrides.get("shard_size") == 5000


@pytest.mark.asyncio
async def test_handle_async_with_node_only(copy_command, mocker, basic_schema, project):
    opts = {**_DEFAULT_OPTIONS, "node-only": True}
    _make_handle_async_setup(copy_command, mocker, project, basic_schema, opts)
    await copy_command.handle_async()
    run_copy_op = copy_command.run_operation.call_args_list[-1].args[0]
    assert run_copy_op.retriever_overrides.get("node_only") is True


@pytest.mark.asyncio
async def test_handle_async_always_loads_schema(
    copy_command, mocker, basic_schema, project
):
    _make_handle_async_setup(
        copy_command, mocker, project, basic_schema, _DEFAULT_OPTIONS
    )
    await copy_command.handle_async()
    project.make_schema_for_copy.assert_called_once()


# ---------------------------------------------------------------------------
# parse_key_value_options tests
# ---------------------------------------------------------------------------


def test_parse_key_value_options_int(copy_command, mocker):
    copy_command.option = mocker.Mock(return_value=["limit=1000"])
    result = copy_command.parse_key_value_options("retriever-option")
    assert result == {"limit": 1000}
    assert isinstance(result["limit"], int)


def test_parse_key_value_options_float(copy_command, mocker):
    copy_command.option = mocker.Mock(return_value=["ratio=0.75"])
    result = copy_command.parse_key_value_options("retriever-option")
    assert result == {"ratio": 0.75}
    assert isinstance(result["ratio"], float)


def test_parse_key_value_options_bool(copy_command, mocker):
    copy_command.option = mocker.Mock(return_value=["enabled=true", "debug=False"])
    result = copy_command.parse_key_value_options("retriever-option")
    assert result == {"enabled": True, "debug": False}


def test_parse_key_value_options_string(copy_command, mocker):
    copy_command.option = mocker.Mock(return_value=["host=localhost"])
    result = copy_command.parse_key_value_options("retriever-option")
    assert result == {"host": "localhost"}
    assert isinstance(result["host"], str)


def test_parse_key_value_options_empty(copy_command, mocker):
    copy_command.option = mocker.Mock(return_value=[])
    result = copy_command.parse_key_value_options("connector-option")
    assert result == {}


def test_parse_key_value_options_none(copy_command, mocker):
    copy_command.option = mocker.Mock(return_value=None)
    result = copy_command.parse_key_value_options("connector-option")
    assert result == {}


def test_parse_key_value_options_multiple(copy_command, mocker):
    copy_command.option = mocker.Mock(
        return_value=["limit=500", "enabled=true", "host=db.example.com", "ratio=0.5"]
    )
    result = copy_command.parse_key_value_options("retriever-option")
    assert result == {
        "limit": 500,
        "enabled": True,
        "host": "db.example.com",
        "ratio": 0.5,
    }
