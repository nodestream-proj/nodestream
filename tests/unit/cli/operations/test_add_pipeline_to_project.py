import pytest

from nodestream.cli.operations import AddPipelineToProject
from nodestream.project import PipelineScope


def test_get_scope_no_scope_name_provided(
    project_with_default_scope, project_dir, default_scope
):
    subject = AddPipelineToProject(project_with_default_scope, project_dir)
    result = subject.get_scope()
    assert result == default_scope


def test_get_scope_no_scope_with_name_exists(
    project_with_default_scope,
    project_dir,
):
    subject = AddPipelineToProject(
        project_with_default_scope, project_dir, "test_scope"
    )
    result = subject.get_scope()
    assert result.name == "test_scope"
    assert result == project_with_default_scope.scopes_by_name["test_scope"]


def test_get_scope_exiting_scope(
    project_with_default_scope, project_dir, default_scope
):
    subject = AddPipelineToProject(project_with_default_scope, project_dir, "default")
    result = subject.get_scope()
    assert result == default_scope


@pytest.mark.asyncio
async def test_perform(project_with_default_scope, project_dir, mocker):
    subject = AddPipelineToProject(project_with_default_scope, project_dir)
    subject.get_scope = mocker.Mock(PipelineScope)
    await subject.perform(None)
    subject.get_scope.return_value.add_pipeline_definition.assert_called_once()
