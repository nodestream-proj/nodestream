from pathlib import Path

import pytest

from nodestream.pipeline import PipelineInitializationArguments
from nodestream.project import (
    PipelineDefinition,
    PipelineProgressReporter,
    PipelineScope,
    Project,
    RunRequest,
)


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
    assert project.scopes_by_name["scope1"] is scopes[0]
    assert project.scopes_by_name["scope2"] is scopes[1]


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
    result = Project.from_file(file_name)
    assert len(result.scopes_by_name) == 1


def test_project_from_file_missing_file():
    file_name = Path("tests/unit/project/fixtures/missing_project.yaml")
    with pytest.raises(FileNotFoundError):
        Project.from_file(file_name)


def test_ensure_modules_are_imported(mocker, project):
    importlib = mocker.patch("nodestream.project.project.importlib")
    project.ensure_modules_are_imported()
    importlib.import_module.assert_has_calls(
        [mocker.call("module1"), mocker.call("module2")]
    )
