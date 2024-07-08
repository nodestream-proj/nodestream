import json
from dataclasses import asdict
from pathlib import Path

import pytest

from nodestream.model import DesiredIngestion
from nodestream.project import PipelineDefinition
from nodestream.schema.printers import SchemaPrinter

@pytest.mark.integration
@pytest.mark.asyncio
def test_schema_introspection(snapshot):
    file_path = Path("tests/integration/fixtures/pipelines/interpreter_schema_tests.yaml")
    definition = PipelineDefinition.from_path(file_path)
    printer = SchemaPrinter.from_name("graphql")
    result = printer.print_schema_to_string(definition.make_schema())
    snapshot.snapshot_dir = "tests/integration/snapshots"
    snapshot_file = f"schema_snapshot_interpreter_schema_tests_graphql.graphql"
    snapshot.assert_match(result, snapshot_file)
