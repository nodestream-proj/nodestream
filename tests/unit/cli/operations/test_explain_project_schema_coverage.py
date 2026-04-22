import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.operations.explain_project_schema import ExplainProjectSchema


@pytest.mark.asyncio
async def test_explain_project_schema_lists_all_pipelines_when_no_filters(
    project_with_default_scope, mocker
):
    command = mocker.Mock()

    operation = ExplainProjectSchema(
        project=project_with_default_scope,
        node_type_name=None,
        relationship_type_name=None,
        scope=None,
    )

    await operation.perform(command)

    # Expect a header line referring to all types across all scopes.
    command.line.assert_called_once_with(
        "Pipelines contributing to all types across all scopes:"
    )

    # And expect that a table is rendered using TableOutputFormat via command.table.
    command.table.assert_called_once()
    headers, rows = command.table.call_args[0]
    assert_that(headers, equal_to(["scope", "name", "file", "targets", "annotations"]))
    assert_that(len(rows), equal_to(1))


@pytest.mark.asyncio
async def test_explain_project_schema_node_and_relationship_with_matches(
    project_with_default_scope, mocker
):
    project = project_with_default_scope
    scope = next(iter(project.scopes_by_name.values()))
    pipeline = next(iter(scope.pipelines_by_name.values()))

    project.explain_node_type = mocker.Mock(return_value=[pipeline.name])
    project.explain_relationship_type = mocker.Mock(return_value=[pipeline.name])
    # Adjacencies show that the requested node type is actually an endpoint of the
    # requested relationship, so the endpoint guard passes and the intersection of
    # provenance sets controls the result.
    project.explain_relationship_adjacencies = mocker.Mock(
        return_value=[mocker.Mock(from_node_type="Person", to_node_type="Movie")]
    )

    command = mocker.Mock()

    operation = ExplainProjectSchema(
        project=project,
        node_type_name="Person",
        relationship_type_name="LIKES",
        scope=None,
    )

    await operation.perform(command)

    # Because there is at least one matching pipeline, we should see a header and table
    # output referring to the specific node and relationship types.
    command.line.assert_called_once_with(
        "Pipelines contributing to node type 'Person' and relationship type 'LIKES' across all scopes:"
    )
    command.table.assert_called_once()


def test_get_filtered_pipelines_filters_by_node_pipelines(mocker):
    project = mocker.Mock()
    scope = mocker.Mock()
    scope.name = "default"

    pipeline_in = mocker.Mock()
    pipeline_in.name = "pipe-in"
    pipeline_out = mocker.Mock()
    pipeline_out.name = "pipe-out"

    scope.pipelines_by_name = {
        pipeline_in.name: pipeline_in,
        pipeline_out.name: pipeline_out,
    }
    project.scopes_by_name = {"default": scope}
    project.explain_node_type = mocker.Mock(return_value=[pipeline_in.name])

    operation = ExplainProjectSchema(
        project=project,
        node_type_name="Person",
        relationship_type_name=None,
        scope=None,
    )

    filtered = list(operation._get_filtered_pipelines())

    assert_that(filtered, equal_to([(scope.name, pipeline_in)]))


def test_get_filtered_pipelines_filters_by_relationship_pipelines(mocker):
    project = mocker.Mock()
    scope = mocker.Mock()
    scope.name = "default"

    pipeline_in = mocker.Mock()
    pipeline_in.name = "pipe-in"
    pipeline_out = mocker.Mock()
    pipeline_out.name = "pipe-out"

    scope.pipelines_by_name = {
        pipeline_in.name: pipeline_in,
        pipeline_out.name: pipeline_out,
    }
    project.scopes_by_name = {"default": scope}
    project.explain_relationship_type = mocker.Mock(return_value=[pipeline_in.name])

    operation = ExplainProjectSchema(
        project=project,
        node_type_name=None,
        relationship_type_name="LIKES",
        scope=None,
    )

    filtered = list(operation._get_filtered_pipelines())

    assert_that(filtered, equal_to([(scope.name, pipeline_in)]))


@pytest.mark.asyncio
async def test_explain_project_schema_no_filters_with_missing_scope(mocker):
    project = mocker.Mock()
    project.scopes_by_name = {
        "default": mocker.Mock(name="default", pipelines_by_name={})
    }

    command = mocker.Mock()

    operation = ExplainProjectSchema(
        project=project,
        node_type_name=None,
        relationship_type_name=None,
        scope="missing",
    )

    await operation.perform(command)

    command.line.assert_called_once_with("No pipelines found. in scope 'missing'.")
