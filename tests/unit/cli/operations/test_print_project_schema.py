from pathlib import Path

import pytest

from nodestream.cli.operations import PrintProjectSchema


@pytest.fixture
def ran_print_project_schema_operation(mocker, project_with_default_scope):
    std_out = mocker.patch(
        "nodestream.schema.printers.SchemaPrinter.print_schema_to_stdout"
    )
    file_out = mocker.patch(
        "nodestream.schema.printers.SchemaPrinter.print_schema_to_file"
    )
    project_with_default_scope.get_schema = mocker.Mock(return_value="schema")

    async def _ran_print_project_schema_operation(output_path):
        operation = PrintProjectSchema(
            project=project_with_default_scope,
            format_string="plain",
            output_file=output_path,
            type_overrides_file=None,
        )
        await operation.perform(mocker.Mock())
        return operation, std_out, file_out

    return _ran_print_project_schema_operation


@pytest.mark.asyncio
async def test_print_project_schema_prints_schema_to_stdout(
    ran_print_project_schema_operation,
):
    _, stdout, fileout = await ran_print_project_schema_operation(None)
    assert stdout.called
    assert not fileout.called


@pytest.mark.asyncio
async def test_print_project_schema_prints_schema_to_file(
    ran_print_project_schema_operation,
):
    _, stdout, fileout = await ran_print_project_schema_operation("some/path")
    assert not stdout.called
    assert fileout.called


@pytest.mark.asyncio
async def test_print_project_schema_with_pipeline_filtering(
    mocker, project_with_default_scope
):
    std_out = mocker.patch(
        "nodestream.schema.printers.SchemaPrinter.print_schema_to_stdout"
    )
    project_with_default_scope.get_schema = mocker.Mock(return_value="full_schema")
    project_with_default_scope.get_pipelines_schema = mocker.Mock(
        return_value="filtered_schema"
    )

    operation = PrintProjectSchema(
        project=project_with_default_scope,
        format_string="plain",
        pipeline_names=["test_pipeline"],
    )
    await operation.perform(mocker.Mock())

    # Should call get_pipelines_schema, not get_schema
    project_with_default_scope.get_pipelines_schema.assert_called_once_with(
        pipeline_names=["test_pipeline"], type_overrides_file=None
    )
    project_with_default_scope.get_schema.assert_not_called()
    std_out.assert_called_once()


@pytest.mark.asyncio
async def test_print_project_schema_with_pipeline_filtering_and_overrides(
    mocker, project_with_default_scope
):
    project_with_default_scope.get_pipelines_schema = mocker.Mock(
        return_value="filtered_schema"
    )

    operation = PrintProjectSchema(
        project=project_with_default_scope,
        format_string="plain",
        type_overrides_file="overrides.yaml",
        pipeline_names=["test_pipeline", "another_pipeline"],
    )
    await operation.perform(mocker.Mock())

    # Should call get_pipelines_schema with correct arguments
    project_with_default_scope.get_pipelines_schema.assert_called_once_with(
        pipeline_names=["test_pipeline", "another_pipeline"],
        type_overrides_file=Path("overrides.yaml"),
    )
