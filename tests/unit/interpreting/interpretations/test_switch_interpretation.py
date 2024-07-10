import pytest
from hamcrest import assert_that, has_entry, not_

from nodestream.interpreting.interpretations.switch_interpretation import (
    INVALID_SWITCH_ERROR_MESSAGE,
    SWITCH_COMPLETENESS_ERROR_MESSAGE,
    SwitchError,
    SwitchInterpretation,
    UnhandledBranchError,
)

INTERPRETATION_USED_AS_HIT = {"type": "properties", "properties": {"success": True}}
INTERPRETATION_FOR_RANDOM = {"type": "properties", "properties": {"random": True}}

HIT_DOCUMENT = {"foo": "bar"}


def test_missing_without_default(blank_context):
    with pytest.raises(UnhandledBranchError):
        subject = SwitchInterpretation(
            switch_on="foo", cases={"not_foo": INTERPRETATION_USED_AS_HIT}
        )
        subject.interpret(blank_context)


def test_missing_with_default(blank_context):
    subject = SwitchInterpretation(
        switch_on="foo",
        cases={"not_foo": INTERPRETATION_USED_AS_HIT},
        default=INTERPRETATION_USED_AS_HIT,
    )
    subject.interpret(blank_context)
    properties = blank_context.desired_ingest.source.properties
    assert_that(properties, has_entry("success", True))
    assert_that(properties, not_(has_entry("random", True)))


def test_missing_without_default_without_error(blank_context):
    subject = SwitchInterpretation(
        switch_on="foo",
        cases={"not_foo": INTERPRETATION_USED_AS_HIT},
        fail_on_unhandled=False,
    )
    subject.interpret(blank_context)
    properties = blank_context.desired_ingest.source.properties
    assert_that(properties, not_(has_entry("success", True)))
    assert_that(properties, not_(has_entry("random", True)))


def test_switch_with_multiple_interpretations(blank_context):
    subject = SwitchInterpretation(
        switch_on="foo",
        cases={"foo": [INTERPRETATION_USED_AS_HIT, INTERPRETATION_FOR_RANDOM]},
        fail_on_unhandled=False,
    )
    subject.interpret(blank_context)
    properties = blank_context.desired_ingest.source.properties
    assert_that(properties, has_entry("success", True))
    assert_that(properties, has_entry("random", True))


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

INCOMPLETE_SWITCH_ARGS = {
    "switch_on": "thing",
    "cases": {
        "case_a": [TEST_SOURCE_NODE_FILE_DATA],
        "case_b": [TEST_RELATIONSHIP_FILE_DATA],
    },
}

INVALID_SWITCH_ARGS = {
    "switch_on": "thing",
    "cases": {
        "case_a": [[TEST_SOURCE_NODE_FILE_DATA], [TEST_SOURCE_NODE_FILE_DATA]],
        "case_b": [[TEST_SOURCE_NODE_FILE_DATA], [TEST_SOURCE_NODE_FILE_DATA]],
    },
}


def test_incomplete_switch_initialization_error():
    with pytest.raises(SwitchError) as error:
        _ = SwitchInterpretation(**INCOMPLETE_SWITCH_ARGS)
        assert error.message == SWITCH_COMPLETENESS_ERROR_MESSAGE


def test_invalid_switch_initialization_error():
    with pytest.raises(SwitchError) as error:
        _ = SwitchInterpretation(**INVALID_SWITCH_ARGS)
        assert error.message == INVALID_SWITCH_ERROR_MESSAGE
