import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.operations import RunCopy


@pytest.fixture
def subject(mocker):
    return RunCopy(
        mocker.Mock(),
        mocker.Mock(),
        mocker.Mock(),
        ["Person", "Movie"],
        ["ACTED_IN"],
    )


def test_build_writer(subject):
    result = subject.build_writer()
    assert_that(result, equal_to(subject.to_target.make_writer.return_value))


def test_build_copier(subject):
    subject.project.gather_used_indexes.return_value = []
    result = subject.build_copier()
    assert_that(
        result.type_retriever,
        equal_to(subject.from_target.make_type_retriever.return_value),
    )
    assert_that(result.node_types, equal_to(subject.node_types))
    assert_that(result.relationship_types, equal_to(subject.relationship_types))


def test_build_pipeline(subject, mocker):
    subject.build_copier = mocker.Mock()
    subject.build_writer = mocker.Mock()
    result = subject.build_pipeline()
    assert_that(
        result.steps,
        equal_to(
            [subject.build_copier.return_value, subject.build_writer.return_value]
        ),
    )


@pytest.mark.asyncio
async def test_perform(subject, mocker):
    pipeline = mocker.AsyncMock()
    subject.build_pipeline = mocker.Mock(return_value=pipeline)
    await subject.perform(mocker.Mock())
    pipeline.run.assert_awaited_once_with()
