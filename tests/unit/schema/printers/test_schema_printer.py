import tempfile
from pathlib import Path

from nodestream.schema.printers import SchemaPrinter
from nodestream.schema.schema import GraphSchema


def test_schema_printer_print_schema_to_stdout(mocker):
    schema = GraphSchema([], [])
    schema_printer = SchemaPrinter()
    schema_printer.print_schema_to_string = mocker.Mock(return_value="schema")
    schema_printer.print_schema_to_stdout(schema, print_mock := mocker.Mock())
    print_mock.assert_called_once_with("schema")
    schema_printer.print_schema_to_string.assert_called_once_with(schema)


def test_schema_printer_print_schema_to_file(mocker):
    with tempfile.NamedTemporaryFile(mode="w") as f:
        file_path = Path(f.name)
        schema = GraphSchema([], [])
        schema_printer = SchemaPrinter()
        schema_printer.print_schema_to_string = mocker.Mock(return_value="schema")
        schema_printer.print_schema_to_file(schema, file_path)
        assert file_path.read_text() == "schema"
