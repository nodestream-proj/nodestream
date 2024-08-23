import tempfile
from pathlib import Path

import pytest

from nodestream.schema import Schema
from nodestream.schema.printers import SchemaPrinter


def test_schema_printer_print_schema_to_stdout(mocker):
    schema = Schema()
    schema_printer = SchemaPrinter()
    schema_printer.print_schema_to_string = mocker.Mock(return_value="schema")
    schema_printer.print_schema_to_stdout(schema, print_mock := mocker.Mock())
    print_mock.assert_called_once_with("schema")
    schema_printer.print_schema_to_string.assert_called_once_with(schema)


@pytest.fixture
def target_file():
    with tempfile.NamedTemporaryFile(mode="r", delete=False) as f:
        name = Path(f.name)
    yield (path := Path(name))
    path.unlink(missing_ok=True)


def test_schema_printer_print_schema_to_file(mocker, target_file):
    schema = Schema()
    schema_printer = SchemaPrinter()
    schema_printer.print_schema_to_string = mocker.Mock(return_value="schema")
    schema_printer.print_schema_to_file(schema, target_file)
    assert target_file.read_text() == "schema"
