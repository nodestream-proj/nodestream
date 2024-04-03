from pathlib import Path

import pytest
from hamcrest import assert_that, has_key, is_, not_

from nodestream.pipeline import (
    PipelineInitializationArguments,
    PipelineProgressReporter,
)
from nodestream.project import PipelineDefinition, PipelineScope, RunRequest
from nodestream.project.pipeline_scope import MissingExpectedPipelineError


@pytest.fixture
def pipelines():
    return [
        PipelineDefinition("pipeline1", Path("path/to/pipeline")),
        PipelineDefinition("pipeline2", Path("path/to/pipeline")),
    ]


@pytest.fixture
def scope(pipelines):
    pipelines_by_name = {p.name: p for p in pipelines}
    return PipelineScope("scope", pipelines_by_name)


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
    assert_that(scope.delete_pipeline("does_not_exist"), is_(False))


def test_delete_pipeline_removed_definition(scope):
    assert_that(scope.delete_pipeline("pipeline1"), is_(True))
    assert_that(scope.pipelines_by_name, not_(has_key("pipeline1")))


def test_delete_pipleine_did_not_remove_file_when_told_to_ignore(scope, mocker):
    scope.pipelines_by_name["pipeline1"].remove_file = rm = mocker.Mock()
    assert_that(
        scope.delete_pipeline("pipeline1", remove_pipeline_file=False), is_(True)
    )
    rm.assert_not_called()


def test_all_subordinate_components(scope, pipelines):
    assert_that(list(scope.get_child_expanders()), is_(pipelines))
