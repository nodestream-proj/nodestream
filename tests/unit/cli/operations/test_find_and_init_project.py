import pytest

from nodestream.cli.commands import NodestreamCommand
from nodestream.cli.operations import InitializeProject
from nodestream.project import Project

from hamcrest import assert_that, equal_to


@pytest.fixture
def subject():
    return InitializeProject()


@pytest.fixture
def command():
    return NodestreamCommand()


@pytest.mark.asyncio
async def test_perform_initializes_project(subject, command, mocker):
    command.get_project = mocker.Mock()
    command.get_project.return_value = project = mocker.Mock(Project)
    result = await subject.perform(command)
    assert_that(result, equal_to(project))
    project.ensure_modules_are_imported.assert_called_once()
