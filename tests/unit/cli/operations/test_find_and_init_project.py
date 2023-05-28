import pytest
from pathlib import Path

from cleo.commands.command import Command

from nodestream.cli.operations.find_and_init_project import (
    FindAndInitializeProject,
    Project,
)


@pytest.fixture
def subject():
    return FindAndInitializeProject()


@pytest.fixture
def command():
    return Command()


@pytest.mark.asyncio
async def test_perform_initializes_project(subject, command, mocker):
    subject.get_project = mocker.Mock()
    subject.get_project.return_value = project = mocker.Mock(Project)
    result = await subject.perform(command)
    assert result == project
    project.ensure_modules_are_imported.assert_called_once()


@pytest.mark.parametrize(
    "input,expected_path", [(None, None), ("some/path.yaml", Path("some/path.yaml"))]
)
def test_get_project_unset(subject, command, mocker, input, expected_path):
    patch_from_file = mocker.patch(
        "nodestream.cli.operations.find_and_init_project.Project.from_file"
    )
    patch_from_file.return_value = project = mocker.Mock()
    path_str = "path/to/file.yaml"
    command.option = mocker.Mock(return_value=path_str)
    result = subject.get_project(command)
    patch_from_file.assert_called_once_with(Path(path_str))
    assert result == project
