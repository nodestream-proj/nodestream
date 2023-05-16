import pytest

from nodestream.pipeline import PipelineInitializationArguments
from nodestream.project import (
    PipelineScope,
    PipelineDefinition,
    RunRequest,
    PipelineProgressReporter,
)


@pytest.fixture
def pipelines():
    return [
        PipelineDefinition("pipeline1", "path/to/pipeline"),
        PipelineDefinition("pipeline2", "path/to/pipeline"),
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
        reporting_arguments=PipelineProgressReporter(),
    )
    request.execute_with_definition = mocker.Mock(return_value=async_return())
    await scope.run_request(request)
    request.execute_with_definition.called_once_with(pipelines[0])
