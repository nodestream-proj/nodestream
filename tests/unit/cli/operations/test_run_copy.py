import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.operations import RunCopy
from nodestream.pipeline import PipelineProgressReporter


@pytest.fixture
def subject(mocker, basic_schema):
    return RunCopy(
        from_target=mocker.Mock(),
        to_target=mocker.Mock(),
        schema=basic_schema,
        node_types=["Person", "Movie"],
        relationship_types=["ACTED_IN"],
    )


def test_build_writer(subject):
    result = subject.build_writer()
    subject.to_target.make_writer.assert_called_once_with(
        connector_overrides={},
        batch_size=1000,
        flush_concurrency=1,
    )
    assert_that(result, equal_to(subject.to_target.make_writer.return_value))


def test_build_copier(subject):
    result = subject.build_copier()
    subject.from_target.make_type_retriever.assert_called_once_with(limit=1000)
    assert_that(result.node_types, equal_to(subject.node_types))
    assert_that(result.relationship_types, equal_to(subject.relationship_types))


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


@pytest.mark.asyncio
async def test_perform(subject, mocker):
    pipeline = mocker.AsyncMock()
    subject.build_pipeline = mocker.Mock(return_value=pipeline)
    await subject.perform(mocker.Mock())
    assert pipeline.run.await_count == 1
    assert isinstance(pipeline.run.await_args[1]["reporter"], PipelineProgressReporter)
