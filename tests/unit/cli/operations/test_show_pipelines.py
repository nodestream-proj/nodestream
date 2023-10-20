from pathlib import Path

import pytest
from hamcrest import assert_that, equal_to, has_length, instance_of

from nodestream.cli.operations.show_pipelines import (
    JsonOutputFormat,
    ShowPipelines,
    TableOutputFormat,
)
from nodestream.project import PipelineDefinition, PipelineScope


@pytest.fixture
def project_with_two_scopes(project_with_default_scope, project_dir):
    another_pipeline = PipelineDefinition("test", project_dir / "test.yaml")
    another_scope = PipelineScope("another", [another_pipeline])
    project_with_default_scope.add_scope(another_scope)
    return project_with_default_scope


def test_show_piplines_get_matching_pipelines_no_scopes_implies_all_scopes(
    project_with_two_scopes,
):
    subject = ShowPipelines(project_with_two_scopes, None)
    results = list(subject.get_matching_pipelines())
    assert_that(results, has_length(2))


def test_show_pipelines_get_matching_pipeline_with_defined_scope_only_gets_that_scope(
    project_with_two_scopes,
):
    subject = ShowPipelines(project_with_two_scopes, "another")
    results = list(subject.get_matching_pipelines())
    assert_that(results, has_length(1))
    assert_that(results[0][0], equal_to("another"))


@pytest.mark.asyncio
async def test_show_pipelines_perform(project_with_two_scopes, mocker):
    subject = ShowPipelines(project_with_two_scopes, None)
    subject.get_output_format = mocker.Mock()
    subject.get_matching_pipelines = mocker.Mock(return_value=[])
    command = mocker.Mock()
    await subject.perform(command)
    subject.get_output_format.assert_called_once_with(command)
    subject.get_output_format.return_value.output.assert_called_once_with([])


def test_show_pipelines_get_format_json(project_with_two_scopes):
    subject = ShowPipelines(project_with_two_scopes, "default", use_json=True)
    assert_that(subject.get_output_format(None), instance_of(JsonOutputFormat))


def test_show_pipelines_get_format_table(project_with_two_scopes):
    subject = ShowPipelines(project_with_two_scopes, "default", use_json=False)
    assert_that(subject.get_output_format(None), instance_of(TableOutputFormat))


def test_output_table_format(project_with_two_scopes, project_dir, mocker):
    results = ShowPipelines(project_with_two_scopes, "another").get_matching_pipelines()
    subject = TableOutputFormat(command := mocker.Mock())
    subject.output(results)
    expected_headers = ["scope", "name", "file", "annotations"]
    expected_rows = [["another", "test", str(project_dir / "test.yaml"), ""]]
    command.table.assert_called_once_with(expected_headers, expected_rows)
    command.table.return_value.render.assert_called_once()


def test_json_output_format(project_with_two_scopes, mocker, project_dir):
    subject = ShowPipelines(project_with_two_scopes, "another")
    results = subject.get_matching_pipelines()
    subject = JsonOutputFormat(command := mocker.Mock())
    command.is_verbose = False
    subject.output(results)
    pipeline_file = str(project_dir / "test.yaml")
    command.write.assert_called_once_with(f'["{pipeline_file}"]')
