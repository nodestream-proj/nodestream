from unittest.mock import call

import pytest
from freezegun import freeze_time
from hamcrest import assert_that, equal_to, has_length, instance_of

from nodestream.interpreting.interpretations import SourceNodeInterpretation
from nodestream.interpreting.interpreter import (
    InterpretationPass,
    Interpreter,
    MultiSequenceInterpretationPass,
    NullInterpretationPass,
    SingleSequenceInterpretationPass,
)
from nodestream.interpreting.record_decomposers import (
    RecordDecomposer,
    WholeRecordDecomposer,
)
from nodestream.model import DesiredIngestion
from nodestream.pipeline import IterableExtractor
from nodestream.pipeline.value_providers import ProviderContext
from nodestream.pipeline.pipeline import empty_async_generator


@pytest.fixture
def stubbed_interpreter(mocker):
    return Interpreter(
        before_iteration=mocker.Mock(InterpretationPass),
        interpretations=mocker.Mock(InterpretationPass),
        decomposer=mocker.Mock(RecordDecomposer),
    )


@pytest.fixture
def single_pass_interpreter():
    return Interpreter(
        before_iteration=NullInterpretationPass(),
        interpretations=SingleSequenceInterpretationPass(
            SourceNodeInterpretation("test", {"test": "test"})
        ),
        decomposer=WholeRecordDecomposer(),
    )


@pytest.mark.asyncio
async def test_test_single_interpreter_yields_index(single_pass_interpreter):
    results = [
        r
        async for r in single_pass_interpreter.handle_async_record_stream(
            empty_async_generator()
        )
    ]
    assert_that(results, has_length(2))


def test_null_interpretation_pass_pass_returns_passed_context():
    null_pass = NullInterpretationPass()
    assert_that(list(null_pass.apply_interpretations(None)), equal_to([None]))


def test_single_sequence_interpretation_pass_returns_passed_context(mocker):
    single_pass = SingleSequenceInterpretationPass(mocker.Mock())
    assert_that(list(single_pass.apply_interpretations(None)), equal_to([None]))
    single_pass.interpretations[0].interpret.assert_called_once_with(None)


def test_interpretation_pass_passing_null_returns_null_interpretation_pass():
    null_pass = InterpretationPass.from_file_data(None)
    assert_that(null_pass, instance_of(NullInterpretationPass))


def test_interpretation_pass_passing_list_of_list_returns_multi_pass():
    multi_pass = InterpretationPass.from_file_data(
        [
            [{"type": "properties", "properties": {}}],
            [{"type": "properties", "properties": {}}],
        ]
    )
    assert_that(multi_pass, instance_of(MultiSequenceInterpretationPass))


def test_intepret_record_iterates_through_interpretation_process(stubbed_interpreter):
    input_record = 1
    stubbed_interpreter.decomposer.decompose_record.return_value = [1, 2, 3]
    stubbed_interpreter.interpretations.apply_interpretations.return_value = [
        "a",
        "b",
        "c",
    ]

    results = list(stubbed_interpreter.interpret_record(input_record))
    assert_that(results, equal_to(["a", "b", "c"] * 3))

    stubbed_interpreter.decomposer.decompose_record.assert_called_once()
    stubbed_interpreter.interpretations.apply_interpretations.assert_has_calls(
        (call(1), call(2), call(3))
    )


@pytest.mark.asyncio
@freeze_time("1998-03-25 12:00:01")
async def test_handle_async_record_stream_returns_iteration_results(
    stubbed_interpreter, mocker
):
    contexts = [[ProviderContext.fresh(i)] for i in range(5)]
    stubbed_interpreter.gather_used_indexes = mocker.Mock(return_value=[])
    stubbed_interpreter.interpret_record = mocker.Mock(side_effect=contexts)
    results = [
        r
        async for r in stubbed_interpreter.handle_async_record_stream(
            IterableExtractor.range(stop=5).extract_records()
        )
        if isinstance(r, DesiredIngestion)
    ]

    assert_that(results, equal_to([context[0].desired_ingest for context in contexts]))
