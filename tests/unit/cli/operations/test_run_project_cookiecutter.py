import pytest


@pytest.mark.asyncio
async def test_perform(mocker, project_dir):
    from nodestream.cli.operations.run_project_cookiecutter import (
        PROJECT_COOKIECUTTER_URL,
        RunProjectCookiecutter,
    )

    mocked_cookiecutter = mocker.patch(
        "nodestream.cli.operations.run_project_cookiecutter.cookiecutter"
    )
    mocker.patch("nodestream.cli.application.get_version", return_value="0.12.0")
    subject = RunProjectCookiecutter("project_name", str(project_dir), "neo4j")
    await subject.perform(mocker.Mock())
    mocked_cookiecutter.assert_called_once_with(
        PROJECT_COOKIECUTTER_URL,
        no_input=True,
        output_dir=str(project_dir),
        extra_context={
            "project_name": "project_name",
            "database": "neo4j",
            "nodestream_version": "0.12.0",
            "nodestream_semver_non_patch_version": "0.12",
        },
    )
