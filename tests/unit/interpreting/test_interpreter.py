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
from nodestream.pipeline.pipeline import empty_async_generator
from nodestream.pipeline.value_providers import ProviderContext


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
    stubbed_interpreter.before_iteration.apply_interpretations.return_value = [
        "base1",
        "base2",
        "base3",
    ]
    stubbed_interpreter.interpretations.apply_interpretations.return_value = [
        "inner1",
        "inner2",
        "inner3",
    ]

    results = list(stubbed_interpreter.interpret_record(input_record))

    # On each source record, the record can be decomposed into multiple records (3 in this case)
    # each of those records can be interpreted with multiple base contexts (before iteration; 3 in this case)
    # each of those items can return multiple results (3 in this case). Hence a total of 27 results (3 * 3 * 3)
    assert_that(results, equal_to(["inner1", "inner2", "inner3"] * 9))

    # We should decompose the record once for each base context we got from before_iteration.
    assert_that(stubbed_interpreter.decomposer.decompose_record.call_count, equal_to(3))

    # The inner interpretations should be called once for each sub-record, for each sub-context.
    # In this case, 3 sub-records, 3 sub-contexts, so 9 calls.
    stubbed_interpreter.interpretations.apply_interpretations.assert_has_calls(
        (call(1), call(2), call(3)) * 3
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
