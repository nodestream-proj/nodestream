import pytest

from nodestream.cli.operations import CommitProjectToDisk


@pytest.mark.asyncio
async def test_commit_project_to_disk(project_with_default_scope, project_dir, mocker):
    project_with_default_scope.write_to_path = mocker.Mock()
    subject = CommitProjectToDisk(project_with_default_scope, project_dir)
    await subject.perform(None)
    project_with_default_scope.write_to_path.assert_called_once_with(project_dir)
