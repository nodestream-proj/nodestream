import os
from copy import deepcopy
from pathlib import Path

import pytest
from hamcrest import (
    assert_that,
    calling,
    contains_inanyorder,
    equal_to,
    has_length,
    raises,
    same_instance,
)

from nodestream.file_io import LazyLoadedArgument
from nodestream.pipeline import (
    PipelineInitializationArguments,
    PipelineProgressReporter,
)
from nodestream.pipeline.scope_config import ScopeConfig
from nodestream.project import (
    PipelineDefinition,
    PipelineScope,
    Project,
    RunRequest,
    Target,
)
from nodestream.project.pipeline_definition import PipelineConfiguration
from nodestream.project.plugin import PluginConfiguration
from nodestream.schema import Schema


@pytest.fixture
def targets():
    return {
        "t1": Target("t1", {"a": "b"}),
    }


@pytest.fixture
def plugins():
    return [
        PluginConfiguration(
            "plugin-name",
            {
                "plugin-pipeline": PipelineDefinition(
                    "plugin-pipeline",
                    Path("path/to/pipeline"),
                    PipelineConfiguration(["t2"], True, {"pipeline-annotations": True}),
                )
            },
            ScopeConfig({"baz": "qux"}),
            PipelineConfiguration(["t1"], False, {"foo": "bar"}),
        )
    ]


@pytest.fixture
def scopes():
    return [
        PipelineScope(
            "scope1",
            {
                "test": PipelineDefinition(
                    "test",
                    Path(
                        "path/to/pipeline",
                    ),
                    PipelineConfiguration(["t1"], False, {"foo": "bar"}),
                )
            },
        ),
        PipelineScope(
            "scope2",
            {"test2": PipelineDefinition("test2", Path("path/to/pipeline"))},
            config=ScopeConfig({"baz": "qux"}),
        ),
    ]


@pytest.fixture
def plugin_scope():
    return PipelineScope("scope3", {})


@pytest.fixture
def project(scopes, plugins, targets):
    scopes_by_name = {scope.name: scope for scope in scopes}
    plugins_by_name = {plugin.name: plugin for plugin in plugins}
    return Project(scopes_by_name, plugins_by_name, targets)


@pytest.fixture
def add_env_var() -> pytest.fixture():
    os.environ["USERNAME_ENV"] = "bob"
    return os.environ["USERNAME_ENV"]


def test_project_dumps_and_reloads_preserves_all_data(project):
    original_dumped = project.to_file_data()
    reloaded = Project.from_file_data(deepcopy(original_dumped))
    dump_after_reload = reloaded.to_file_data()
    assert_that(dump_after_reload, equal_to(original_dumped))


def test_pipeline_organizes_scopes_by_name(scopes, project):
    assert_that(project.scopes_by_name["scope1"], same_instance(scopes[0]))
    assert_that(project.scopes_by_name["scope2"], same_instance(scopes[1]))


@pytest.mark.asyncio
async def test_project_runs_pipeline_in_scope_when_present(
    mocker, async_return, project, scopes
):
    scopes[0].run_request = mocker.Mock(return_value=async_return())
    request = RunRequest(
        "test", PipelineInitializationArguments(), PipelineProgressReporter()
    )
    await project.run(request)
    scopes[0].run_request.assert_called_once_with(request)


def test_project_init_sets_up_plugin_scope_when_present(plugin_scope):
    project = Project(
        {plugin_scope.name: plugin_scope},
        {},
    )
    plugin_config = PluginConfiguration(
        name="plugin_scope",
        config=ScopeConfig({"PluginUsername": "bob"}),
    )
    project.add_plugin(plugin_config)
    project.add_plugin_scope("plugin_scope", plugin_config)
    assert project.plugins_by_name["plugin_scope"].config == ScopeConfig(
        {"PluginUsername": "bob"}
    )
    assert_that(
        project.scopes_by_name["plugin_scope"].config.get_config_value(
            "PluginUsername"
        ),
        equal_to("bob"),
    )


def test_project_init_doesnt_set_up_plugin_scope_when_non_matching_name_present(
    plugin_scope,
):
    project = Project(
        {plugin_scope.name: plugin_scope},
        {},
    )
    plugin_config = PluginConfiguration(
        name="other",
        config=ScopeConfig({"PluginUsername": "bob"}),
    )
    project.add_plugin(plugin_config)
    project.add_plugin_scope("plugin_scope", plugin_config)
    assert project.scopes_by_name.get("plugin_scope") is None


def test_project_from_with_with_targets():
    result = Project.read_from_file(
        Path(
            "nodestream/nodestream/tests/unit/project/fixtures/simple_project_with_targets.yaml"
        )
    )
    assert_that(result.targets_by_name, equal_to({"t1": Target("t1", {"a": "b"})}))


def test_project_from_file_with_config(add_env_var):
    file_name = Path(
        "nodestream/nodestream/tests/unit/project/fixtures/simple_project_with_config.yaml"
    )
    result: Project = Project.read_from_file(file_name)
    assert_that(result.scopes_by_name, has_length(1))

    assert_that(result.plugins_by_name["test"].name, equal_to("test"))
    assert_that(
        result.plugins_by_name["test"].config.get_config_value("PluginUsername"),
        equal_to("bob"),
    )
    assert_that(
        result.scopes_by_name["perpetual"].config.get_config_value("Username"),
        equal_to("bob"),
    )


def test_project_from_file_with_config_targets(add_env_var):
    result = Project.read_from_file(
        Path(
            "nodestream/nodestream/tests/unit/project/fixtures/simple_project_with_config_targets.yaml"
        )
    )
    assert_that(
        result.targets_by_name,
        equal_to({"t1": Target("t1", {"a": "b"}), "t2": Target("t2", {"c": "d"})}),
    )
    assert_that(
        result.scopes_by_name["config_targets_only"].config.get_config_value(
            "Username"
        ),
        equal_to("bob"),
    )
    assert_that(
        result.scopes_by_name["config_targets_only"]
        .pipelines_by_name["target-pipeline"]
        .configuration.effective_targets,
        contains_inanyorder(*["t1"]),
    )


def test_project_from_file_with_scope_targets(add_env_var):
    result = Project.read_from_file(
        Path(
            "nodestream/nodestream/tests/unit/project/fixtures/simple_project_with_config_targets.yaml"
        )
    )
    assert_that(
        result.targets_by_name,
        equal_to({"t1": Target("t1", {"a": "b"}), "t2": Target("t2", {"c": "d"})}),
    )
    assert_that(
        result.scopes_by_name["scope_targets"].config.get_config_value("Username"),
        equal_to("bob"),
    )
    assert_that(
        result.scopes_by_name["scope_targets"]
        .pipelines_by_name["scope-target-pipeline"]
        .configuration.effective_targets,
        contains_inanyorder(*["t1"]),
    )
    assert_that(
        result.scopes_by_name["scope_targets"]
        .pipelines_by_name["scope-and-config-target-pipeline"]
        .configuration.effective_targets,
        contains_inanyorder(*["t1", "t2"]),
    )
    assert_that(
        result.scopes_by_name["overlapping_targets"]
        .pipelines_by_name["scope-and-config-target-pipeline"]
        .configuration.effective_targets,
        contains_inanyorder(*["t1", "t2"]),
    )
    assert_that(
        result.scopes_by_name["exclude_inherited_targets"]
        .pipelines_by_name["scope-and-config-target-pipeline"]
        .configuration.effective_targets,
        contains_inanyorder(*["t2"]),
    )


def test_project_from_file():
    file_name = Path(
        "nodestream/nodestream/tests/unit/project/fixtures/simple_project.yaml"
    )
    result = Project.read_from_file(file_name)
    assert_that(result.scopes_by_name, has_length(1))
    assert_that(
        result.scopes_by_name["perpetual"].config,
        equal_to(None),
    )


def test_project_from_file_missing_file():
    file_name = Path(
        "nodestream/nodestream/tests/unit/project/fixtures/missing_project.yaml"
    )
    with pytest.raises(FileNotFoundError):
        Project.read_from_file(file_name)


def test_get_scopes_by_name_none_returns_all_scopes(project, scopes):
    assert_that(list(project.get_scopes_by_name(None)), equal_to(scopes))


def test_get_scopes_by_name_named_scope_is_only_one_returned(project, scopes):
    assert_that(list(project.get_scopes_by_name("scope1")), equal_to([scopes[0]]))


def test_get_scopes_by_name_misssing_scope_returns_nothing(project):
    assert_that(list(project.get_scopes_by_name("missing")), has_length(0))


def test_delete_pipeline_forwards_deletes_to_appropriate_scope(project, scopes, mocker):
    scopes[0].delete_pipeline = scope_delete_pipeline = mocker.Mock()
    scopes[1].delete_pipeline = not_expected = mocker.Mock()
    project.delete_pipeline("scope1", "test")
    scope_delete_pipeline.assert_called_once_with(
        "test", remove_pipeline_file=True, missing_ok=True
    )
    not_expected.assert_not_called()


def test_get_schema_no_overrides(project, mocker):
    project.make_schema = mocker.Mock(Schema)
    project.get_schema()
    project.make_schema.assert_called_once()
    project.make_schema.return_value.apply_overrides.assert_not_called()


def test_get_schema_with_overrides(project, mocker):
    project.make_schema = mocker.Mock(Schema)
    Schema.read_from_file = mocker.Mock(Schema)
    project.get_schema(Path("some/path"))
    project.make_schema.assert_called_once()
    project.make_schema.return_value.merge.assert_called_once_with(
        Schema.read_from_file.return_value
    )


@pytest.mark.asyncio
async def test_get_snapshot_for(project, mocker):
    project.run = mocker.AsyncMock()
    await project.get_snapshot_for("pipeline")
    project.run.assert_awaited_once()


def test_all_pipeline_names(project):
    pipeline_names = [pipeline.name for pipeline in project.get_all_pipelines()]
    assert_that(pipeline_names, equal_to(["test", "test2"]))


def test_get_target_present(project):
    assert_that(project.get_target_by_name("t1"), equal_to(Target("t1", {"a": "b"})))


def test_get_target_missing(project):
    assert_that(
        calling(project.get_target_by_name).with_args("missing"), raises(ValueError)
    )


def test_project_load_lifecycle_hooks_calls_all_hooks_on_all_plugins(mocker):
    plugins = [mocker.Mock(), mocker.Mock()]
    mocker.patch(
        "nodestream.project.project.ProjectPlugin.all_active",
        return_value=plugins,
    )

    file_path = Path(
        "nodestream/nodestream/tests/unit/project/fixtures/simple_project.yaml"
    )
    project = Project.read_from_file(file_path)

    for plugin in plugins:
        plugin.before_project_load.assert_called_once_with(file_path)
        plugin.activate.assert_called_once_with(project)
        plugin.after_project_load.assert_called_once_with(project)


def test_load_plugin_from_resources():
    file_path = Path(
        "nodestream/nodestream/tests/unit/project/fixtures/simple_project_with_config.yaml"
    )
    project = Project.read_from_file(file_path)
    project.add_plugin_scope_from_pipeline_resources(
        "test", "nodestream.tests.unit.project.fixtures.pipelines"
    )
    plugin_scope = project.scopes_by_name["test"]
    assert_that(project.plugins_by_name["test"].name, equal_to("test"))

    # loads scope properly
    assert_that(plugin_scope.name, equal_to("test"))
    assert_that(
        plugin_scope.config,
        equal_to(
            ScopeConfig(
                config={
                    "PluginUsername": LazyLoadedArgument(
                        tag="env", value="USERNAME_ENV"
                    )
                }
            )
        ),
    )


def test_project_load_and_reload():
    project = Project.read_from_file(
        Path(
            "nodestream/nodestream/tests/unit/project/fixtures/simple_project_with_config_targets.yaml"
        )
    )
    project_data = project.to_file_data()
    reloaded = Project.validate_and_load(project_data)
    assert_that(reloaded, equal_to(project))
