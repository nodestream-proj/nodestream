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
    assert result is subject.to_target.make_writer.return_value


def test_build_copier(subject):
    result = subject.build_copier()
    call_kwargs = subject.from_target.make_type_retriever.call_args[1]
    assert_that(call_kwargs["schema"], equal_to(subject.schema))
    assert_that(call_kwargs["node_types"], equal_to(["Person", "Movie"]))
    assert_that(call_kwargs["relationship_types"], equal_to(["ACTED_IN"]))
    from nodestream.databases.copy import Copier

    assert type(result) is Copier


def test_build_pipeline(subject, mocker):
    subject.build_copier = mocker.Mock()
    subject.build_writer = mocker.Mock()
    result = subject.build_pipeline()
    assert_that(
        result.steps,
        equal_to(
            (subject.build_copier.return_value, subject.build_writer.return_value)
        ),
    )


def test_build_copier_node_only(mocker, basic_schema):
    """node_only passed via retriever_overrides is forwarded to make_type_retriever."""
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
        retriever_overrides={"node_only": True},
    )
    op.build_copier()
    call_kwargs = from_target.make_type_retriever.call_args[1]
    assert_that(call_kwargs["node_types"], equal_to(["Person", "Movie"]))
    assert_that(call_kwargs["relationship_types"], equal_to(["ACTED_IN"]))
    assert_that(call_kwargs["node_only"], equal_to(True))


@pytest.mark.asyncio
async def test_perform(subject, mocker):
    pipeline = mocker.AsyncMock()
    subject.build_pipeline = mocker.Mock(return_value=pipeline)
    await subject.perform(mocker.Mock())
    reporter_arg = pipeline.run.await_args[1]["reporter"]
    assert isinstance(reporter_arg, PipelineProgressReporter)
