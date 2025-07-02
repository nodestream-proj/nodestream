import pytest
from hamcrest import assert_that, equal_to
from unittest.mock import Mock

from nodestream.cli.operations.run_pipeline import (
    WARNING_NO_TARGETS_PROVIDED,
    JsonProgressIndicator,
    ProgressIndicator,
    RunPipeline,
    SpinnerProgressIndicator,
)
from nodestream.metrics import Metrics
from nodestream.project import PipelineConfiguration, PipelineDefinition, Project


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
    # Updated to handle all the option calls including time-interval-seconds
    option_responses = {
        "storage-backend": "my-storage",
        "annotations": annotations,
        "step-outbox-size": "10001",
        "target": targets,
        "time-interval-seconds": None,  # No time interval provided
        "reporting-frequency": "10000"
    }
    command.option.side_effect = lambda opt: option_responses.get(opt)
    command.has_json_logging_set = False  # Required for create_progress_reporter
    command.is_very_verbose = False  # Required for make_run_request
    command.argument.return_value = [pipeline_name]
    pipeline = mocker.patch("nodestream.project.PipelineDefinition")
    pipeline.name = pipeline_name
    result = run_pipeline_operation.make_run_request(command, pipeline)
    assert_that(result.pipeline_name, equal_to(pipeline_name))
    assert_that(result.initialization_arguments.annotations, equal_to(annotations))
    assert_that(result.initialization_arguments.step_outbox_size, equal_to(10001))
    assert_that(result.progress_reporter.reporting_frequency, equal_to(10000))


def test_create_progress_reporter_with_time_interval_seconds(run_pipeline_operation, mocker):
    """Test that time_interval_seconds gets properly converted to float and passed to PipelineProgressReporter"""
    command = mocker.Mock()
    command.option.side_effect = lambda opt: {
        "time-interval-seconds": "30.5",
        "reporting-frequency": "1000"
    }.get(opt)
    command.has_json_logging_set = False
    
    # Mock PipelineProgressReporter to capture arguments
    mock_progress_reporter = mocker.patch("nodestream.cli.operations.run_pipeline.PipelineProgressReporter")
    
    result = run_pipeline_operation.create_progress_reporter(command, "test_pipeline")
    
    # Verify PipelineProgressReporter was called with correct time_interval_seconds
    mock_progress_reporter.assert_called_once()
    call_args = mock_progress_reporter.call_args
    assert_that(call_args.kwargs["time_interval_seconds"], equal_to(30.5))
    assert_that(call_args.kwargs["reporting_frequency"], equal_to(1000))


def test_create_progress_reporter_without_time_interval_seconds(run_pipeline_operation, mocker):
    """Test that time_interval_seconds is None when not provided"""
    command = mocker.Mock()
    command.option.side_effect = lambda opt: {
        "time-interval-seconds": None,
        "reporting-frequency": "2000"
    }.get(opt)
    command.has_json_logging_set = False
    
    # Mock PipelineProgressReporter to capture arguments
    mock_progress_reporter = mocker.patch("nodestream.cli.operations.run_pipeline.PipelineProgressReporter")
    
    result = run_pipeline_operation.create_progress_reporter(command, "test_pipeline")
    
    # Verify PipelineProgressReporter was called with None for time_interval_seconds
    mock_progress_reporter.assert_called_once()
    call_args = mock_progress_reporter.call_args
    assert_that(call_args.kwargs["time_interval_seconds"], equal_to(None))
    assert_that(call_args.kwargs["reporting_frequency"], equal_to(2000))


def test_create_progress_reporter_with_json_indicator(run_pipeline_operation, mocker):
    """Test that create_progress_reporter works correctly with JSON progress indicator"""
    command = mocker.Mock()
    command.option.side_effect = lambda opt: {
        "time-interval-seconds": "15.0",
        "reporting-frequency": "500"
    }.get(opt)
    command.has_json_logging_set = True
    
    # Mock PipelineProgressReporter to capture arguments
    mock_progress_reporter = mocker.patch("nodestream.cli.operations.run_pipeline.PipelineProgressReporter")
    
    result = run_pipeline_operation.create_progress_reporter(command, "test_pipeline")
    
    # Verify PipelineProgressReporter was called with correct arguments
    mock_progress_reporter.assert_called_once()
    call_args = mock_progress_reporter.call_args
    assert_that(call_args.kwargs["time_interval_seconds"], equal_to(15.0))
    assert_that(call_args.kwargs["reporting_frequency"], equal_to(500))


def test_make_run_request_with_time_interval_seconds_integration(run_pipeline_operation, mocker):
    """Integration test to ensure make_run_request properly handles time_interval_seconds through create_progress_reporter"""
    annotations = ["annotation1"]
    targets = ["t1"]
    pipeline_name = "my_pipeline"
    command = mocker.Mock()
    
    # Setup command.option to handle all the different option calls made by make_run_request
    option_responses = {
        "storage-backend": "my-storage",
        "annotations": annotations,
        "step-outbox-size": "10001",
        "target": targets,
        "time-interval-seconds": "45.0",
        "reporting-frequency": "5000"
    }
    command.option.side_effect = lambda opt: option_responses.get(opt)
    command.has_json_logging_set = False
    command.is_very_verbose = False
    command.argument.return_value = [pipeline_name]
    
    pipeline = mocker.Mock()
    pipeline.name = pipeline_name
    pipeline.configuration = PipelineConfiguration()
    
    # Mock the project's get_object_storage_by_name method
    run_pipeline_operation.project.get_object_storage_by_name.return_value = None
    run_pipeline_operation.project.get_target_by_name.return_value = None
    
    # Mock PipelineProgressReporter to capture its arguments
    mock_progress_reporter = mocker.patch("nodestream.cli.operations.run_pipeline.PipelineProgressReporter")
    
    result = run_pipeline_operation.make_run_request(command, pipeline)
    
    # Verify the progress reporter was created with correct time_interval_seconds
    mock_progress_reporter.assert_called_once()
    call_args = mock_progress_reporter.call_args
    assert_that(call_args.kwargs["time_interval_seconds"], equal_to(45.0))
    assert_that(call_args.kwargs["reporting_frequency"], equal_to(5000))
    
    # Verify other parts of the request are still correct
    assert_that(result.pipeline_name, equal_to(pipeline_name))
    assert_that(result.initialization_arguments.annotations, equal_to(annotations))
    assert_that(result.initialization_arguments.step_outbox_size, equal_to(10001))


def test_spinner_on_start(mocker):
    spinner = SpinnerProgressIndicator(mocker.Mock(), "pipeline_name")
    spinner.on_start()
    spinner.command.progress_indicator.assert_called_once()
    spinner.progress.start.assert_called_once()


def test_spinner_on_finish(mocker):
    spinner = SpinnerProgressIndicator(mocker.Mock(), "pipeline_name")
    spinner.on_start()
    spinner.on_finish(Metrics())
    spinner.progress.finish.assert_called_once()


def test_spinner_progress_callback(mocker):
    spinner = SpinnerProgressIndicator(mocker.Mock(), "pipeline_name")
    spinner.on_start()
    mock_metrics = mocker.Mock()
    spinner.progress_callback(1000, mock_metrics)
    spinner.progress.set_message.assert_called_once()
    mock_metrics.tick.assert_called_once()


def test_spinner_error_condition(mocker):
    spinner = SpinnerProgressIndicator(mocker.Mock(), "pipeline_name")
    spinner.on_start()
    spinner.on_fatal_error(Exception())
    spinner.progress.set_message.assert_called_once()
    mock_metrics = mocker.Mock()
    with pytest.raises(Exception):
        spinner.on_finish(mock_metrics)
    mock_metrics.tick.assert_called_once()


@pytest.mark.parametrize(
    "from_cli,from_pipeline,expected",
    [
        (set(), set(), set()),
        (["t1", "t2"], set(), {"t1", "t2"}),
        (set(), {"t1", "t2"}, {"t1", "t2"}),
        (["t1", "t2"], {"t2", "t3"}, {"t1", "t2", "t3"}),
    ],
)
def test_combine_targets_from_command_and_pipeline(
    mocker, from_cli, from_pipeline, expected
):
    command = mocker.Mock()
    pipeline = PipelineDefinition(
        None, None, configuration=PipelineConfiguration(targets=from_pipeline)
    )
    command.option.return_value = from_cli
    result = RunPipeline(None).combine_targets_from_command_and_pipeline(
        command, pipeline
    )
    assert_that(result, equal_to(expected))


def test_combine_targets_from_command_and_pipeline_warns_when_targets_not_set(mocker):
    command = mocker.Mock()
    pipeline = PipelineDefinition(None, None, configuration=PipelineConfiguration())
    command.option.return_value = []
    RunPipeline(None).combine_targets_from_command_and_pipeline(command, pipeline)
    command.line.assert_called_once_with(WARNING_NO_TARGETS_PROVIDED)


@pytest.mark.parametrize(
    "provided_pipelines,expected",
    [
        (None, ["dummy"]),  # No pipelines provided
        (["dummy", "p2"], ["dummy"]),  # Partially found pipelines
        (["p1", "p2"], []),  # Pipelines not found in project
        (["dummy"], ["dummy"]),  # Single pipeline provided
    ],
)
def test_get_pipleines_to_run(
    provided_pipelines, expected, project_with_default_scope, mocker
):
    cmd = mocker.Mock()
    cmd.argument.return_value = provided_pipelines
    results = RunPipeline(project_with_default_scope).get_pipelines_to_run(cmd)
    assert_that([result.name for result in results], equal_to(expected))


def test_progress_indicator_error(mocker):
    indicator = ProgressIndicator(mocker.Mock(), "pipeline_name")
    indicator.on_fatal_error(Exception("Boom"))
    indicator.command.line.assert_called_with("<error>Boom</error>")


def test_json_progress_indicator(mocker):
    indicator = JsonProgressIndicator(mocker.Mock(), "pipeline_name")
    indicator.logger.info = mocker.Mock()
    indicator.on_start()
    indicator.on_finish(Metrics())
    assert indicator.logger.info.call_args_list == [
        mocker.call("Starting Pipeline"),
        mocker.call("Pipeline Completed"),
    ]


def test_json_progress_indicator_on_fatal_error(mocker):
    indicator = JsonProgressIndicator(mocker.Mock(), "pipeline_name")
    indicator.logger.error = mocker.Mock()
    exception = Exception("Boom")
    indicator.on_fatal_error(exception)
    indicator.logger.error.assert_called_once_with(
        "Pipeline Failed", exc_info=exception
    )


def test_json_progress_indicator_on_progress(mocker):
    indicator = JsonProgressIndicator(mocker.Mock(), "pipeline_name")
    indicator.logger.info = mocker.Mock()
    indicator.progress_callback(1000, Metrics())
    indicator.logger.info.assert_called_once_with(
        "Processing Record", extra={"index": 1000}
    )


def test_json_progress_indicator_on_finish_with_exception(mocker):
    indicator = JsonProgressIndicator(mocker.Mock(), "pipeline_name")
    indicator.logger.info = mocker.Mock()
    indicator.logger.error = mocker.Mock()
    exception = Exception("Boom")
    indicator.on_fatal_error(exception)
    with pytest.raises(Exception):
        indicator.on_finish(Metrics())
    indicator.logger.error.assert_called_once_with(
        "Pipeline Failed", exc_info=exception
    )
    indicator.logger.info.assert_called_once_with("Pipeline Completed")
    assert indicator.exception is exception
