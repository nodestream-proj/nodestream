from pathlib import Path

import pytest

from nodestream.project import (
    Project,
    RunRequest,
    PipelineScope,
    PipelineDefinition,
    PipelineProgressReporter,
)
from nodestream.pipeline import PipelineInitializationArguments


@pytest.fixture
def scopes():
    return [
        PipelineScope("scope1", [PipelineDefinition("test", Path("path/to/pipeline"))]),
        PipelineScope("scope2", []),
    ]


@pytest.fixture
def project(scopes):
    return Project(scopes)


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
    file_name = "tests/unit/project/fixtures/simple_project.yaml"
    result = Project.from_file(file_name)
    assert len(result.scopes_by_name) == 1
