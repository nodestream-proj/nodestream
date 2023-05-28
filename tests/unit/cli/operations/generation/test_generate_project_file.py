import pytest

from nodestream.cli.operations.generation.generate_project_file import (
    GenerateProjectFile,
)


@pytest.fixture
def python_files(project_dir):
    files = ["proj/test1.py", "proj/test2.py", "proj/__init__.py"]
    return [project_dir / file for file in files]


@pytest.fixture
def pipeline_files(project_dir):
    return [project_dir / "pipelines" / "test.yaml"]


@pytest.fixture
def generate_project_file_command(project_dir, python_files, pipeline_files):
    return GenerateProjectFile(project_dir, pipeline_files, python_files, "neo4j")


def test_generate_project_file_command_generate_import_directives(
    generate_project_file_command,
):
    assert generate_project_file_command.generate_import_directives() == [
        "proj.test1",
        "proj.test2",
        "nodestream.databases.neo4j",
    ]


def test_generate_pipeline_scope(
    generate_project_file_command, pipeline_files, project_dir
):
    result = generate_project_file_command.generate_pipeline_scope()
    assert result.name == "default"
    assert result.pipelines_by_name["test"].file_path == pipeline_files[0].relative_to(
        project_dir
    )


@pytest.mark.asyncio
async def test_perform(generate_project_file_command, project_dir):
    await generate_project_file_command.perform(None)
    assert (project_dir / "nodestream.yaml").exists
