import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.operations import RunCopy
from nodestream.pipeline import PipelineProgressReporter


@pytest.fixture
def subject(mocker, basic_schema):
    from_target = mocker.Mock()
    from_target.name = "source"
    to_target = mocker.Mock()
    to_target.name = "destination"
    return RunCopy(
        from_target=from_target,
        to_target=to_target,
        schema=basic_schema,
        node_types=["Person", "Movie"],
        relationship_types=["ACTED_IN"],
    )


def test_build_writer(subject):
    result = subject.build_writer()
    # Verify that the result is the writer returned by the target.
    assert result is subject.to_target.make_writer.return_value


@pytest.mark.asyncio
async def test_build_copier(subject):
    result = await subject.build_copier()
    # node_types and relationship_types are always forwarded; everything else
    # flows through retriever_overrides.
    call_kwargs = subject.from_target.make_type_retriever.call_args[1]
    assert_that(call_kwargs["node_types"], equal_to(["Person", "Movie"]))
    assert_that(call_kwargs["relationship_types"], equal_to(["ACTED_IN"]))
    from nodestream.databases.copy import Copier

    assert type(result) is Copier


@pytest.mark.asyncio
async def test_build_pipeline(subject, mocker):
    subject.build_copier = mocker.AsyncMock()
    subject.build_writer = mocker.Mock()
    result = await subject.build_pipeline()
    assert_that(
        result.steps,
        equal_to(
            (subject.build_copier.return_value, subject.build_writer.return_value)
        ),
    )


@pytest.mark.asyncio
async def test_build_copier_relationships_only(mocker, basic_schema):
    """relationships_only passed via retriever_overrides is forwarded to make_type_retriever."""
    from_target = mocker.Mock()
    from_target.name = "source"
    to_target = mocker.Mock()
    to_target.name = "destination"
    op = RunCopy(
        from_target=from_target,
        to_target=to_target,
        schema=basic_schema,
        node_types=["Person", "Movie"],
        relationship_types=["ACTED_IN"],
        retriever_overrides={"relationships_only": True},
    )
    await op.build_copier()
    call_kwargs = from_target.make_type_retriever.call_args[1]
    assert_that(call_kwargs["node_types"], equal_to(["Person", "Movie"]))
    assert_that(call_kwargs["relationship_types"], equal_to(["ACTED_IN"]))
    assert_that(call_kwargs["relationships_only"], equal_to(True))


@pytest.mark.asyncio
async def test_perform(subject, mocker):
    pipeline = mocker.AsyncMock()
    subject.build_pipeline = mocker.AsyncMock(return_value=pipeline)
    await subject.perform(mocker.Mock())
    # Verify run was awaited with a proper progress reporter.
    reporter_arg = pipeline.run.await_args[1]["reporter"]
    assert isinstance(reporter_arg, PipelineProgressReporter)
