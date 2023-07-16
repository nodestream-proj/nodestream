from pathlib import Path

import pytest
from hamcrest import assert_that, equal_to, has_length, same_instance

from nodestream.pipeline import (
    PipelineInitializationArguments,
    PipelineProgressReporter,
)
from nodestream.project import PipelineDefinition, PipelineScope, Project, RunRequest
from nodestream.schema.schema import GraphSchema


@pytest.fixture
def scopes():
    return [
        PipelineScope("scope1", [PipelineDefinition("test", Path("path/to/pipeline"))]),
        PipelineScope("scope2", []),
    ]


@pytest.fixture
def imports():
    return ["module1", "module2"]


@pytest.fixture
def project(scopes, imports):
    return Project(scopes, imports)


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


def test_project_from_file():
    file_name = Path("tests/unit/project/fixtures/simple_project.yaml")
    result = Project.read_from_file(file_name)
    assert_that(result.scopes_by_name, has_length(1))


def test_project_from_file_missing_file():
    file_name = Path("tests/unit/project/fixtures/missing_project.yaml")
    with pytest.raises(FileNotFoundError):
        Project.read_from_file(file_name)


def test_ensure_modules_are_imported(mocker, project):
    importlib = mocker.patch("nodestream.project.project.importlib")
    project.ensure_modules_are_imported()
    importlib.import_module.assert_has_calls(
        [mocker.call("module1"), mocker.call("module2")]
    )


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
    project.generate_graph_schema = mocker.Mock(GraphSchema)
    project.get_schema()
    project.generate_graph_schema.assert_called_once()
    project.generate_graph_schema.return_value.apply_overrides.assert_not_called()


def test_get_schema_with_overrides(project, mocker):
    project.generate_graph_schema = mocker.Mock(GraphSchema)
    project.get_schema("some/path")
    project.generate_graph_schema.assert_called_once()
    project.generate_graph_schema.return_value.apply_type_overrides_from_file.assert_called_once_with(
        "some/path"
    )
