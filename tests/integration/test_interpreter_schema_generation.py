from pathlib import Path

import pytest

from nodestream.project import PipelineDefinition
from nodestream.schema.printers import SchemaPrinter


@pytest.mark.integration
def test_schema_introspection_without_additional_types(snapshot):
    file_path = Path(
        "tests/integration/fixtures/pipelines/interpreter_schema_test.yaml"
    )
    definition = PipelineDefinition.from_path(file_path)
    printer = SchemaPrinter.from_name("graphql")
    # Preserve existing behavior: do not include additional types in this snapshot.
    result = printer.print_schema_to_string(
        definition.make_schema(include_additional_types=False)
    )
    snapshot.snapshot_dir = "tests/integration/snapshots"
    snapshot_file = "schema_snapshot_interpreter_schema_tests_graphql.graphql"
    snapshot.assert_match(result, snapshot_file)
