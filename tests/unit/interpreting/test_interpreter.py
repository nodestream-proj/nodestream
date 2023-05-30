import pytest

from nodestream.pipeline.pipeline import empty_asnyc_generator
from nodestream.interpreting import SourceNodeInterpretation
from nodestream.interpreting.interpreter import (
    Interpreter,
    NullInterpretationPass,
    MultiSequenceInterpretationPass,
    SingleSequenceIntepretationPass,
    InterpretationPass,
)


@pytest.fixture
def single_pass_interpreter():
    return Interpreter(
        global_enrichment=NullInterpretationPass(),
        interpretations=SingleSequenceIntepretationPass(
            SourceNodeInterpretation("test", {"test": "test"})
        ),
        iterate_on=None,
    )


@pytest.mark.asyncio
async def test_test_single_interpreter_yields_index(single_pass_interpreter):
    results = [
        r
        async for r in single_pass_interpreter.handle_async_record_stream(
            empty_asnyc_generator()
        )
    ]
    assert len(results) == 2


def test_null_interpretation_pass_pass_returns_passed_context():
    null_pass = NullInterpretationPass()
    assert list(null_pass.apply_interpretations(None)) == [None]


def test_single_sequence_interpretation_pass_returns_passed_context(mocker):
    single_pass = SingleSequenceIntepretationPass(mocker.Mock())
    assert list(single_pass.apply_interpretations(None)) == [None]
    single_pass.interpretations[0].interpret.assert_called_once_with(None)


def test_interpretation_pass_passing_null_returns_null_interpretation_pass():
    null_pass = InterpretationPass.from_file_arguments(None)
    assert isinstance(null_pass, NullInterpretationPass)


def test_interpretation_pass_passing_list_of_list_returns_multi_pass():
    multi_pass = InterpretationPass.from_file_arguments([[None], [None]])
    assert isinstance(multi_pass, MultiSequenceInterpretationPass)
