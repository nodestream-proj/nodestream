import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.operations.run_pipeline import RunPipeline, SpinnerProgressIndicator
from nodestream.pipeline.meta import PipelineContext
from nodestream.project import Project


@pytest.fixture
def run_pipeline_operation(mocker):
    return RunPipeline(mocker.AsyncMock(Project))


@pytest.mark.asyncio
async def test_run_pipeline_operation_perform(run_pipeline_operation, mocker):
    run_req = "run"
    cmd = mocker.Mock()
    cmd.argument.return_value = ["pipeline_name"]
    run_pipeline_operation.make_run_request = mocker.Mock(return_value=run_req)
    await run_pipeline_operation.perform(cmd)
    run_pipeline_operation.project.run.assert_awaited_once_with(run_req)


def test_make_run_request(run_pipeline_operation, mocker):
    annotations = ["annotation1", "annotation2"]
    targets = ["t1", "t2"]
    pipeline_name = "my_pipeline"
    command = mocker.Mock()
    command.option.side_effect = [annotations, "10001", targets, "10000"]
    command.argument.return_value = [pipeline_name]
    pipeline = mocker.patch("nodestream.project.PipelineDefinition")
    pipeline.name = pipeline_name
    result = run_pipeline_operation.make_run_request(command, pipeline)
    assert_that(result.pipeline_name, equal_to(pipeline_name))
    assert_that(result.initialization_arguments.annotations, equal_to(annotations))
    assert_that(result.initialization_arguments.step_outbox_size, equal_to(10001))
    assert_that(result.progress_reporter.reporting_frequency, equal_to(10000))


def test_spinner_on_start(mocker):
    spinner = SpinnerProgressIndicator(mocker.Mock(), "pipeline_name")
    spinner.on_start()
    spinner.command.progress_indicator.assert_called_once()
    spinner.progress.start.assert_called_once()


def test_spinner_on_finish(mocker):
    spinner = SpinnerProgressIndicator(mocker.Mock(), "pipeline_name")
    spinner.on_start()
    spinner.on_finish(PipelineContext())
    spinner.progress.finish.assert_called_once()


def test_spinner_progress_callback(mocker):
    spinner = SpinnerProgressIndicator(mocker.Mock(), "pipeline_name")
    spinner.on_start()
    spinner.progress_callback(1000, None)
    spinner.progress.set_message.assert_called_once()
