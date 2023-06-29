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
