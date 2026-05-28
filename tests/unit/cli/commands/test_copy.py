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


def test_get_type_selection_from_user_from_option(copy_command, mocker, basic_schema):
    copy_command.option = mocker.Mock(side_effect=[False, ["Person"]])
    types = copy_command.get_type_selection_from_user(basic_schema.nodes, "node")
    assert_that(types, equal_to(["Person"]))
    copy_command.option.assert_called_with("node")


def test_get_type_selection_from_user_from_prompt(copy_command, mocker, basic_schema):
    copy_command.option = mocker.Mock(side_effect=[False, None])
    copy_command.choice = mocker.Mock(return_value=["Person"])
    types = copy_command.get_type_selection_from_user(basic_schema.nodes, "node")
    assert_that(types, equal_to(["Person"]))
    copy_command.option.assert_called_with("node")
    copy_command.choice.assert_called_with(
        "Which node types would you like to copy? (You can select multiple by separating them with a comma)",
        ["Person", "Organization"],
        multiple=True,
    )


def test_get_type_selection_from_user_from_all_flag(copy_command, mocker, basic_schema):
    copy_command.option = mocker.Mock(return_value=True)
    types = copy_command.get_type_selection_from_user(basic_schema.nodes, "node")
    assert_that(types, equal_to(["Person", "Organization"]))
    copy_command.option.assert_called_once_with("all")


def test_get_type_selection_from_user_from_option_unknown_type(
    copy_command, mocker, basic_schema
):
    copy_command.line_error = mocker.Mock()
    with pytest.raises(UnknownTargetError):
        copy_command.option = mocker.Mock(side_effect=[False, ["Unknown"]])
        copy_command.get_type_selection_from_user(basic_schema.nodes, "node")
    copy_command.line_error.assert_called_once_with(
        "Unknown node type: Unknown. Valid options are: Person, Organization"
    )


@pytest.mark.asyncio
async def test_handle_async_unknown_target_error(copy_command, mocker):
    # assume we load the project and the user selects a target that doesn't exist
    copy_command.line_error = mocker.Mock()
    copy_command.run_operation = mocker.AsyncMock()
    copy_command.get_taget_from_user = mocker.Mock(side_effect=UnknownTargetError)
    # InitializeLogger, InitializeMetricsHandler, InitializeProject = 3 operations
    # then get_taget_from_user raises, so RunCopy is never called.
    result = await copy_command.handle_async()
    assert_that(result, equal_to(1))
    assert copy_command.run_operation.await_count == 3
    # Verify no RunCopy operation was created.
    run_copy_calls = [
        c
        for c in copy_command.run_operation.call_args_list
        if isinstance(c.args[0], RunCopy)
    ]
    assert len(run_copy_calls) == 0


@pytest.mark.asyncio
async def test_handle_async(copy_command, mocker, basic_schema, project):
    project.make_schema_for_copy = mocker.Mock(return_value=basic_schema)
    copy_command.line = mocker.Mock()
    copy_command.run_operation = mocker.AsyncMock(
        side_effect=[None, None, project, None]
    )
    copy_command.get_taget_from_user = mocker.Mock(
        side_effect=[project.targets_by_name["test"], project.targets_by_name["test2"]]
    )
    copy_command.get_type_selection_from_user = mocker.Mock(
        side_effect=[["Person", "Organization"], ["BEST_FRIEND_OF", "HAS_EMPLOYEE"]]
    )

    option_values = {
        "all": False,
        "node": [],
        "relationship": [],
        "concurrency-limit": "1",
        "batch-size": "1000",
        "step-outbox-size": "10000",
        "flush-concurrency": "1",
        "connector-option": [],
        "retriever-option": [],
        "shard-size": None,
        "relationships-only": False,
        "reporting-frequency": "1000",
        "metrics-interval-in-seconds": None,
    }
    copy_command.option = mocker.Mock(side_effect=lambda name: option_values[name])
    mocker.patch.object(
        type(copy_command),
        "has_json_logging_set",
        new_callable=mocker.PropertyMock,
        return_value=False,
    )

    await copy_command.handle_async()
    # InitializeLogger, InitializeMetricsHandler, InitializeProject, RunCopy = 4
    assert copy_command.run_operation.await_count == 4

    # The last operation should be a RunCopy with the expected arguments.
    run_copy_call = copy_command.run_operation.call_args_list[-1]
    run_copy_op = run_copy_call.args[0]
    assert isinstance(run_copy_op, RunCopy)
    assert run_copy_op.from_target == project.targets_by_name["test"]
    assert run_copy_op.to_target == project.targets_by_name["test2"]
    assert run_copy_op.node_types == ["Person", "Organization"]
    assert run_copy_op.relationship_types == ["BEST_FRIEND_OF", "HAS_EMPLOYEE"]
    assert run_copy_op.batch_size == 1000
    assert run_copy_op.flush_concurrency == 1
    assert run_copy_op.connector_overrides == {}
    assert run_copy_op.retriever_overrides.get("concurrency_limit") == 1


@pytest.mark.asyncio
async def test_handle_async_with_non_default_options(
    copy_command, mocker, basic_schema, project
):
    """Conditional output lines should fire when concurrency/overrides are non-default."""
    project.make_schema_for_copy = mocker.Mock(return_value=basic_schema)
    copy_command.line = mocker.Mock()
    copy_command.run_operation = mocker.AsyncMock(
        side_effect=[None, None, project, None]
    )
    copy_command.get_taget_from_user = mocker.Mock(
        side_effect=[project.targets_by_name["test"], project.targets_by_name["test2"]]
    )
    copy_command.get_type_selection_from_user = mocker.Mock(
        side_effect=[["Person", "Organization"], ["BEST_FRIEND_OF"]]
    )

    option_values = {
        "all": False,
        "node": [],
        "relationship": [],
        "concurrency-limit": "4",
        "batch-size": "1000",
        "step-outbox-size": "10000",
        "flush-concurrency": "3",
        "connector-option": ["uri=bolt://remote:7687"],
        "retriever-option": ["limit=500", "sample_ratio=50"],
        "shard-size": None,
        "relationships-only": False,
        "reporting-frequency": "1000",
        "metrics-interval-in-seconds": None,
    }
    copy_command.option = mocker.Mock(side_effect=lambda name: option_values[name])
    mocker.patch.object(
        type(copy_command),
        "has_json_logging_set",
        new_callable=mocker.PropertyMock,
        return_value=False,
    )

    await copy_command.handle_async()

    run_copy_op = copy_command.run_operation.call_args_list[-1].args[0]
    assert isinstance(run_copy_op, RunCopy)
    assert run_copy_op.flush_concurrency == 3
    assert run_copy_op.connector_overrides == {"uri": "bolt://remote:7687"}
    assert run_copy_op.retriever_overrides.get("concurrency_limit") == 4
    assert run_copy_op.retriever_overrides.get("limit") == 500
    assert run_copy_op.retriever_overrides.get("sample_ratio") == 50

    # Verify the conditional output lines were printed.
    printed = [str(c) for c in copy_command.line.call_args_list]
    assert any("Concurrency Limit" in s for s in printed)
    assert any("Flush Concurrency" in s for s in printed)
    assert any("Connector Overrides" in s for s in printed)
    assert any("Retriever Options" in s for s in printed)


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
