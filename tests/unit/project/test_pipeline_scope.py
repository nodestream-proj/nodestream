from pathlib import Path

import pytest

from nodestream.exceptions import MissingExpectedPipelineError
from nodestream.pipeline import PipelineInitializationArguments
from nodestream.project import (
    PipelineDefinition,
    PipelineProgressReporter,
    PipelineScope,
    RunRequest,
)


@pytest.fixture
def pipelines():
    return [
        PipelineDefinition("pipeline1", Path("path/to/pipeline")),
        PipelineDefinition("pipeline2", Path("path/to/pipeline")),
    ]


@pytest.fixture
def scope(pipelines):
    return PipelineScope("scope", pipelines)


def test_pipeline_scope_organizes_pipeines_by_name(scope, pipelines):
    assert scope["pipeline1"] is pipelines[0]
    assert scope["pipeline2"] is pipelines[1]


@pytest.mark.asyncio
async def test_pipeline_scope_runs_pipeline_when_present(
    scope, mocker, async_return, pipelines
):
    request = RunRequest(
        pipeline_name="pipeline1",
        initialization_arguments=PipelineInitializationArguments(),
        progress_reporter=PipelineProgressReporter(),
    )
    request.execute_with_definition = mocker.Mock(return_value=async_return())
    await scope.run_request(request)
    request.execute_with_definition.called_once_with(pipelines[0])


def test_delete_pipeline_raises_error_when_missing_not_ok(scope):
    with pytest.raises(MissingExpectedPipelineError):
        scope.delete_pipeline("does_not_exist", missing_ok=False)


def test_delete_pipeline_does_not_raise_an_error_when_missing_ok(scope):
    assert scope.delete_pipeline("does_not_exist") is False


def test_delete_pipeline_removed_definition(scope):
    assert scope.delete_pipeline("pipeline1")
    assert "pipeline1" not in scope.pipelines_by_name


def test_delete_pipleine_did_not_remove_file_when_told_to_ignore(scope, mocker):
    scope.pipelines_by_name["pipeline1"].remove_file = rm = mocker.Mock()
    assert scope.delete_pipeline("pipeline1", remove_pipeline_file=False)
    rm.assert_not_called()
