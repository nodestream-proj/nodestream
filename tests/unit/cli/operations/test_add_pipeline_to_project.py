import pytest

from nodestream.cli.operations import AddPipelineToProject
from nodestream.project import PipelineScope

from hamcrest import assert_that, equal_to


def test_get_scope_no_scope_name_provided(
    project_with_default_scope, project_dir, default_scope
):
    subject = AddPipelineToProject(project_with_default_scope, project_dir)
    assert_that(subject.get_scope(), equal_to(default_scope))


def test_get_scope_no_scope_with_name_exists(
    project_with_default_scope,
    project_dir,
):
    subject = AddPipelineToProject(
        project_with_default_scope, project_dir, "test_scope"
    )
    result = subject.get_scope()
    assert_that(result.name, equal_to("test_scope"))
    assert_that(
        result, equal_to(project_with_default_scope.scopes_by_name["test_scope"])
    )


def test_get_scope_exiting_scope(
    project_with_default_scope, project_dir, default_scope
):
    subject = AddPipelineToProject(project_with_default_scope, project_dir, "default")
    assert_that(subject.get_scope(), equal_to(default_scope))


@pytest.mark.asyncio
async def test_perform(project_with_default_scope, project_dir, mocker):
    subject = AddPipelineToProject(project_with_default_scope, project_dir)
    subject.get_scope = mocker.Mock(PipelineScope)
    await subject.perform(None)
    subject.get_scope.return_value.add_pipeline_definition.assert_called_once()
