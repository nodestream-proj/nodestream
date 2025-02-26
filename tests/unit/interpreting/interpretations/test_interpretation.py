from hamcrest import assert_that, instance_of

from nodestream.interpreting.interpretations import (
    Interpretation,
    SourceNodeInterpretation,
    ConditionedInterpretation,
)
from nodestream.interpreting.interpretations.conditions import (
    AlwaysTrue,
    ProviderContext,
)


def test_from_file_data_gets_right_subclass():
    result = Interpretation.from_file_data(
        type="source_node", node_type="Test", key={"key": "value"}
    )
    assert_that(result, instance_of(SourceNodeInterpretation))


def test_from_file_data_gets_condition_handled():
    result = Interpretation.from_file_data(
        type="source_node",
        node_type="Test",
        key={"key": "value"},
        condition={"type": "true"},
    )
    assert_that(result, instance_of(ConditionedInterpretation))
    assert_that(result.condition, instance_of(AlwaysTrue))
    assert_that(result.interpretation, instance_of(SourceNodeInterpretation))


def test_conditioned_interpretation_continues_on_condition_matched(mocker):
    context = mocker.Mock(spec=ProviderContext)
    interpretation = mocker.Mock(spec=SourceNodeInterpretation)
    conditioned = ConditionedInterpretation(AlwaysTrue(), interpretation)
    conditioned.interpret(context)
    interpretation.interpret.assert_called_once_with(context)


def test_conditioned_interpretation_breaks_on_missing_condition(mocker):
    context = mocker.Mock(spec=ProviderContext)
    interpretation = mocker.Mock(spec=SourceNodeInterpretation)
    definitely_totally_true = mocker.Mock(spec=AlwaysTrue)
    definitely_totally_true.evaluate.return_value = False
    conditioned = ConditionedInterpretation(definitely_totally_true, interpretation)
    conditioned.interpret(context)
    interpretation.interpret.assert_not_called()
