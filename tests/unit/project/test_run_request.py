import pytest

from nodestream.pipeline import (
    PipelineInitializationArguments,
    PipelineProgressReporter,
)
from nodestream.project import RunRequest


@pytest.mark.asyncio
async def test_execute_with_definition(mocker):
    request = RunRequest(
        "test", PipelineInitializationArguments(), PipelineProgressReporter()
    )
    pipeline_definition = mocker.Mock()
    pipeline_definition.initialize.return_value = pipeline = mocker.AsyncMock()
    await request.execute_with_definition(pipeline_definition)
    pipeline_definition.initialize.assert_called_once_with(
        request.initialization_arguments
    )
    pipeline.run.assert_awaited_once_with(request.progress_reporter)
