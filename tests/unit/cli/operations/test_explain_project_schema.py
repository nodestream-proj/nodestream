from unittest.mock import Mock

import pytest

from nodestream.cli.operations.explain_project_schema import (
    ExplainProjectSchema,
)


@pytest.mark.asyncio
async def test_explain_project_schema_writes_one_pipeline_per_line(mocker):
    project = mocker.Mock()
    project.explain_node_type.return_value = ["pipe1", "pipe2"]

    command = Mock()

    operation = ExplainProjectSchema(
        project=project,
        kind="node",
        type_name="Person",
        scope=None,
    )

    await operation.perform(command)

    # Expect: header plus one line per pipeline
    command.line.assert_any_call(
        "Pipelines contributing to node type 'Person' across all scopes:"
    )
    command.line.assert_any_call("pipe1")
    command.line.assert_any_call("pipe2")


@pytest.mark.asyncio
async def test_explain_project_schema_no_pipelines_with_scope(mocker):
    project = mocker.Mock()
    project.explain_node_type.return_value = []

    command = Mock()

    operation = ExplainProjectSchema(
        project=project,
        kind="node",
        type_name="Person",
        scope="myscope",
    )

    await operation.perform(command)

    command.line.assert_called_once_with(
        "No pipelines found for node type 'Person' in scope 'myscope'."
    )


@pytest.mark.asyncio
async def test_explain_project_schema_relationships_across_scopes(mocker):
    project = mocker.Mock()
    project.explain_relationship_type.return_value = ["pipe1"]

    command = Mock()

    operation = ExplainProjectSchema(
        project=project,
        kind="relationship",
        type_name="LIKES",
        scope=None,
    )

    await operation.perform(command)

    command.line.assert_any_call(
        "Pipelines contributing to relationship type 'LIKES' across all" " scopes:"
    )
    command.line.assert_any_call("pipe1")
