from os import environ
from unittest.mock import Mock, call

import pytest
from freezegun import freeze_time
from hamcrest import assert_that, equal_to

from nodestream.interpreting.interpretation_passes import (
    NullInterpretationPass,
    SingleSequenceInterpretationPass,
)
from nodestream.interpreting.interpretations import SourceNodeInterpretation
from nodestream.interpreting.interpreter import (
    INTERPRETER_UNIQUENESS_ERROR_MESSAGE,
    VALIDATION_FLAG,
    InterpretationPass,
    Interpreter,
    InterpreterError,
)
from nodestream.interpreting.record_decomposers import (
    RecordDecomposer,
    WholeRecordDecomposer,
)
from nodestream.pipeline import IterableExtractor
from nodestream.pipeline.value_providers import ProviderContext


@pytest.fixture
def single_pass_interpreter():
    return Interpreter(
        before_iteration=NullInterpretationPass(),
        interpretations=SingleSequenceInterpretationPass(
            SourceNodeInterpretation("test", {"test": "test"})
        ),
        decomposer=WholeRecordDecomposer(),
    )


@pytest.fixture
def stubbed_interpreter(mocker):
    return Interpreter(
        before_iteration=mocker.Mock(InterpretationPass),
        interpretations=mocker.Mock(InterpretationPass),
        decomposer=mocker.Mock(RecordDecomposer),
    )


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
async def test_transform_record_returns_iteration_results(stubbed_interpreter, mocker):
    contexts = [[ProviderContext.fresh(i)] for i in range(5)]
    stubbed_interpreter.interpret_record = mocker.Mock(side_effect=contexts)
    results = [
        r
        async for input in IterableExtractor.range(stop=5).extract_records()
        async for r in stubbed_interpreter.transform_record(input)
    ]

    assert_that(results, equal_to([context[0].desired_ingest for context in contexts]))


TEST_SOURCE_NODE_FILE_DATA = {
    "type": "source_node",
    "node_type": "Test",
    "key": {"test_key": "test_value"},
}
TEST_RELATIONSHIP_FILE_DATA = {
    "type": "relationship",
    "node_type": "Test",
    "relationship_type": "TEST_REL",
    "node_key": {"test_key": "test_value"},
}

TEST_INTERPRETER_FILE_DATA = {
    "before_iteration": [TEST_SOURCE_NODE_FILE_DATA, TEST_RELATIONSHIP_FILE_DATA],
    "interpretations": [TEST_RELATIONSHIP_FILE_DATA, TEST_RELATIONSHIP_FILE_DATA],
}

FAILED_TEST_INTERPRETER_FILE_DATA = {
    "before_iteration": [TEST_SOURCE_NODE_FILE_DATA, TEST_RELATIONSHIP_FILE_DATA],
    "interpretations": [TEST_SOURCE_NODE_FILE_DATA, TEST_RELATIONSHIP_FILE_DATA],
}


class MockCoordinator:
    def __init__(self):
        self.final_caller = None


def modify_caller_before_iteration(coordinator):
    coordinator.final_caller = "before_iteration"


def modify_caller_interpretations(coordinator):
    coordinator.final_caller = "interpretations"


def test_interpreter_schema_expansion_expands_the_source_generator_last():
    subject = Interpreter.from_file_data(**TEST_INTERPRETER_FILE_DATA)
    subject.before_iteration.expand_schema = Mock(
        side_effect=modify_caller_before_iteration
    )
    subject.interpretations.expand_schema = Mock(
        side_effect=modify_caller_interpretations
    )
    coordinator = MockCoordinator()
    subject.expand_schema(coordinator)
    assert coordinator.final_caller == "before_iteration"


@pytest.fixture
def environment_flag():
    environ[VALIDATION_FLAG] = "True"


def test_interpreter_verifies_source_node_uniqueness(environment_flag):
    with pytest.raises(InterpreterError) as error:
        _ = Interpreter.from_file_data(**FAILED_TEST_INTERPRETER_FILE_DATA)
        assert error.message == INTERPRETER_UNIQUENESS_ERROR_MESSAGE
