import pytest

from nodestream.cli.operations import RemovePipelineFromProject
from nodestream.project import Project


@pytest.mark.asyncio
async def test_remove_pipeline_from_project_perform(mocker):
    project = mocker.Mock(Project)
    subject = RemovePipelineFromProject(project, "scope", "pipeline")
    await subject.perform(None)
    project.delete_pipeline.assert_called_once_with("scope", "pipeline")
