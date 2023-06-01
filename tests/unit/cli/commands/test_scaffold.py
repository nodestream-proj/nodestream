import pytest

from nodestream.cli.commands import Scaffold


@pytest.fixture
def scaffold_command():
    return Scaffold()


@pytest.mark.asyncio
async def test_handle_async(scaffold_command, mocker, project_dir):
    scaffold_command.argument = mocker.Mock()
    scaffold_command.option = mocker.Mock()
    scaffold_command.run_operation = mocker.AsyncMock()
    scaffold_command.get_project_path = mocker.Mock(return_value=project_dir)
    scaffold_command.argument.return_value = "my_pipeline_thats_generated"
    scaffold_command.option.side_effect = ["scope", "neo4j"]
    await scaffold_command.handle_async()
