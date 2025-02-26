import pytest
from hamcrest import assert_that, is_, instance_of, has_length

from nodestream.pipeline.value_providers import (
    ProviderContext,
    ValueProvider,
    JmespathValueProvider,
)

from nodestream.interpreting.interpretations.conditions import (
    AlwaysTrue,
    Or,
    And,
    Not,
    EqualsOperator,
    GreaterThanOperator,
    LessThanOperator,
    ContainsOperator,
    Comparator,
    Condition,
)


@pytest.fixture
def mock_context(mocker):
    return mocker.Mock(spec=ProviderContext)


def test_always_true(mock_context):
    condition = AlwaysTrue()
    assert_that(condition.evaluate(mock_context), is_(True))


def test_or_condition(mocker, mock_context):
    true_condition = AlwaysTrue()
    false_condition = mocker.Mock(spec=AlwaysTrue)
    false_condition.evaluate.return_value = False

    condition = Or(true_condition, false_condition)
    assert_that(condition.evaluate(mock_context), is_(True))

    condition = Or(false_condition, false_condition)
    assert_that(condition.evaluate(mock_context), is_(False))


def test_and_condition(mocker, mock_context):
    true_condition = AlwaysTrue()
    false_condition = mocker.Mock(spec=AlwaysTrue)
    false_condition.evaluate.return_value = False

    condition = And(true_condition, false_condition)
    assert_that(condition.evaluate(mock_context), is_(False))

    condition = And(true_condition, true_condition)
    assert_that(condition.evaluate(mock_context), is_(True))


def test_not_condition(mocker, mock_context):
    true_condition = AlwaysTrue()
    false_condition = mocker.Mock(spec=AlwaysTrue)
    false_condition.evaluate.return_value = False

    condition = Not(true_condition)
    assert_that(condition.evaluate(mock_context), is_(False))

    condition = Not(false_condition)
    assert_that(condition.evaluate(mock_context), is_(True))


def test_equals_operator():
    operator = EqualsOperator()
    assert_that(operator.operate(1, 1), is_(True))
    assert_that(operator.operate(1, 2), is_(False))


def test_greater_than_operator():
    operator = GreaterThanOperator()
    assert_that(operator.operate(2, 1), is_(True))
    assert_that(operator.operate(1, 2), is_(False))


def test_less_than_operator():
    operator = LessThanOperator()
    assert_that(operator.operate(1, 2), is_(True))
    assert_that(operator.operate(2, 1), is_(False))


def test_contains_operator():
    operator = ContainsOperator()
    assert_that(operator.operate([1, 2, 3], 2), is_(True))
    assert_that(operator.operate([1, 2, 3], 4), is_(False))


def test_comparator(mocker, mock_context):
    left_value = mocker.Mock(spec=ValueProvider)
    right_value = mocker.Mock(spec=ValueProvider)
    left_value.single_value.return_value = 1
    right_value.single_value.return_value = 1

    operator = EqualsOperator()
    condition = Comparator(left_value, right_value, operator)
    assert_that(condition.evaluate(mock_context), is_(True))

    right_value.single_value.return_value = 2
    assert_that(condition.evaluate(mock_context), is_(False))


def test_from_file_data_or():
    data = {"type": "or", "conditions": [{"type": "true"}, {"type": "true"}]}
    condition = Condition.from_tagged_file_data(**data)
    assert_that(condition, instance_of(Or))
    assert_that(condition.conditions, has_length(2))


def test_from_file_data_and():
    data = {"type": "and", "conditions": [{"type": "true"}, {"type": "true"}]}
    condition = Condition.from_tagged_file_data(**data)
    assert_that(condition, instance_of(And))
    assert_that(condition.conditions, has_length(2))


def test_from_file_data_not():
    data = {"type": "not", "condition": {"type": "true"}}
    condition = Condition.from_tagged_file_data(**data)
    assert_that(condition, instance_of(Not))
    assert_that(condition.condition, instance_of(AlwaysTrue))


def test_from_file_data_comparator():
    data = {
        "type": "compare",
        "left": 1,
        "right": JmespathValueProvider.from_string_expression("foo"),
        "operator": "equals",
    }
    condition = Condition.from_tagged_file_data(**data)
    assert_that(condition, instance_of(Comparator))
    assert_that(condition.left, instance_of(ValueProvider))
    assert_that(condition.right, instance_of(ValueProvider))
    assert_that(condition.operator, instance_of(EqualsOperator))
