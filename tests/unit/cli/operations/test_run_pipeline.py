import pytest

from nodestream.cli.operations.run_pipeline import RunPipeline, SpinnerProgressIndicator
from nodestream.project import Project
from nodestream.pipeline import PipelineInitializationArguments


@pytest.fixture
def run_pipeline_operation(mocker):
    return RunPipeline(mocker.AsyncMock(Project))


@pytest.mark.asyncio
async def test_run_pipeline_operation_perform(run_pipeline_operation, mocker):
    run_req = "run"
    run_pipeline_operation.make_run_request = mocker.Mock(return_value=run_req)
    await run_pipeline_operation.perform(mocker.Mock())
    run_pipeline_operation.project.run.assert_awaited_once_with(run_req)


def test_make_run_request(run_pipeline_operation, mocker):
    command = mocker.Mock()
    command.option.side_effect = [["annotation1", "annotation2"], False, "10000"]
    command.argument.return_value = "my_pipeline"
    result = run_pipeline_operation.make_run_request(command)
    assert result.pipeline_name == "my_pipeline"
    assert result.initialization_arguments == PipelineInitializationArguments(
        annotations=["annotation1", "annotation2"]
    )
    assert result.progress_reporter.reporting_frequency == 10000


def test_spinner_on_start(mocker):
    spinner = SpinnerProgressIndicator(mocker.Mock())
    spinner.on_start()
    spinner.command.progress_indicator.assert_called_once()
    spinner.progress.start.assert_called_once()


def test_spinner_on_finish(mocker):
    spinner = SpinnerProgressIndicator(mocker.Mock())
    spinner.on_start()
    spinner.on_finish()
    spinner.progress.finish.assert_called_once()


def test_spinner_progress_callback(mocker):
    spinner = SpinnerProgressIndicator(mocker.Mock())
    spinner.on_start()
    spinner.progress_callback(1000, None)
    spinner.progress.set_message.assert_called_once()
