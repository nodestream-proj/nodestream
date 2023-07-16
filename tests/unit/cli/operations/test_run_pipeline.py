import pytest
from hamcrest import assert_that, equal_to, has_key

from nodestream.cli.operations.run_pipeline import RunPipeline, SpinnerProgressIndicator
from nodestream.pipeline import PipelineInitializationArguments
from nodestream.pipeline.meta import PipelineContext, get_context, reset_listeners
from nodestream.project import Project


@pytest.fixture
def run_pipeline_operation(mocker):
    return RunPipeline(mocker.AsyncMock(Project))


@pytest.mark.asyncio
async def test_run_pipeline_operation_perform(run_pipeline_operation, mocker):
    run_req = "run"
    run_pipeline_operation.init_prometheus_server_if_needed = mocker.Mock()
    run_pipeline_operation.make_run_request = mocker.Mock(return_value=run_req)
    await run_pipeline_operation.perform(mocker.Mock())
    run_pipeline_operation.project.run.assert_awaited_once_with(run_req)


def test_make_run_request(run_pipeline_operation, mocker):
    command = mocker.Mock()
    command.option.side_effect = [["annotation1", "annotation2"], "10000"]
    command.argument.return_value = "my_pipeline"
    result = run_pipeline_operation.make_run_request(command)
    assert_that(result.pipeline_name, equal_to("my_pipeline"))
    assert_that(
        result.initialization_arguments,
        equal_to(
            PipelineInitializationArguments(annotations=["annotation1", "annotation2"])
        ),
    )
    assert_that(result.progress_reporter.reporting_frequency, equal_to(10000))


def test_spinner_on_start(mocker):
    spinner = SpinnerProgressIndicator(mocker.Mock())
    spinner.on_start()
    spinner.command.progress_indicator.assert_called_once()
    spinner.progress.start.assert_called_once()


def test_spinner_on_finish(mocker):
    spinner = SpinnerProgressIndicator(mocker.Mock())
    spinner.on_start()
    spinner.on_finish(PipelineContext())
    spinner.progress.finish.assert_called_once()


def test_spinner_progress_callback(mocker):
    spinner = SpinnerProgressIndicator(mocker.Mock())
    spinner.on_start()
    spinner.progress_callback(1000, None)
    spinner.progress.set_message.assert_called_once()


def test_init_prometheus_server_if_needed_bails_if_unneeded(
    run_pipeline_operation, mocker
):
    start_http_server = mocker.patch(
        "nodestream.cli.operations.run_pipeline.start_http_server"
    )
    command = mocker.Mock()
    command.option.return_value = False
    run_pipeline_operation.init_prometheus_server_if_needed(command)
    start_http_server.assert_not_called()
    command.option.assert_called_once_with("prometheus")


def test_init_prometheus_server_uses_valid_arguments(run_pipeline_operation, mocker):
    start_http_server = mocker.patch(
        "nodestream.cli.operations.run_pipeline.start_http_server"
    )
    command = mocker.Mock()
    command.option.side_effect = [True, "8081", "127.0.0.1"]
    run_pipeline_operation.init_prometheus_server_if_needed(command)
    start_http_server.assert_called_once_with(8081, "127.0.0.1")
    reset_listeners()


def test_metrics_are_submitted_to_prometheus(run_pipeline_operation, mocker):
    mocker.patch("nodestream.cli.operations.run_pipeline.start_http_server")
    command = mocker.Mock()
    command.option.side_effect = [True, "8081", "127.0.0.1"]
    run_pipeline_operation.init_prometheus_server_if_needed(command)
    get_context().increment_stat("stat")

    assert_that(run_pipeline_operation.metric_summaries, has_key("stat"))
    reset_listeners()
