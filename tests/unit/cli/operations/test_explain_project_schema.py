from unittest.mock import Mock

import pytest

from nodestream.cli.operations.explain_project_schema import (
    ExplainProjectSchema,
)


@pytest.mark.asyncio
async def test_explain_project_schema_node_only_no_matching_pipelines(mocker):
    project = mocker.Mock()
    project.scopes_by_name = {
        "default": mocker.Mock(name="default", pipelines_by_name={})
    }
    project.explain_node_type.return_value = []

    command = Mock()

    operation = ExplainProjectSchema(
        project=project,
        node_type_name="Person",
        relationship_type_name=None,
        scope=None,
    )

    await operation.perform(command)

    command.line.assert_any_call(
        "No pipelines found for node type 'Person' across all scopes."
    )


@pytest.mark.asyncio
async def test_explain_project_schema_no_pipelines_with_scope(mocker):
    project = mocker.Mock()
    project.scopes_by_name = {"myscope": mocker.Mock(pipelines_by_name={})}
    project.explain_node_type.return_value = []

    command = Mock()

    operation = ExplainProjectSchema(
        project=project,
        node_type_name="Person",
        relationship_type_name=None,
        scope="myscope",
    )

    await operation.perform(command)

    command.line.assert_called_once_with(
        "No pipelines found for node type 'Person' in scope 'myscope'."
    )


@pytest.mark.asyncio
async def test_explain_project_schema_relationships_across_scopes(mocker):
    project = mocker.Mock()
    project.scopes_by_name = {
        "default": mocker.Mock(name="default", pipelines_by_name={})
    }
    project.explain_relationship_type.return_value = []

    command = Mock()

    operation = ExplainProjectSchema(
        project=project,
        node_type_name=None,
        relationship_type_name="LIKES",
        scope=None,
    )

    await operation.perform(command)

    command.line.assert_any_call(
        "No pipelines found for relationship type 'LIKES' across all scopes."
    )


@pytest.mark.asyncio
async def test_explain_project_schema_node_and_relationship_intersection_message(
    mocker,
):
    project = mocker.Mock()
    project.scopes_by_name = {
        "default": mocker.Mock(name="default", pipelines_by_name={})
    }
    project.explain_node_type.return_value = []
    project.explain_relationship_type.return_value = []

    command = Mock()

    operation = ExplainProjectSchema(
        project=project,
        node_type_name="Person",
        relationship_type_name="LIKES",
        scope=None,
    )

    await operation.perform(command)

    command.line.assert_called_once_with(
        "No pipelines found that contribute to both node type 'Person' and "
        "relationship type 'LIKES' across all scopes."
    )
