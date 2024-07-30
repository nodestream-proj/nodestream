from os import environ

import pytest
from hamcrest import assert_that, equal_to, instance_of

from nodestream.interpreting.interpretation_passes import (
    COMPLETENESS_ERROR_MESSAGE,
    UNIQUENESS_ERROR_MESSAGE,
    VALIDATION_FLAG,
    InterpretationPassError,
    MultiSequenceInterpretationPass,
    NullInterpretationPass,
    SingleSequenceInterpretationPass,
)
from nodestream.interpreting.interpretations import (
    PropertiesInterpretation,
    RelationshipInterpretation,
    SourceNodeInterpretation,
    SwitchInterpretation,
)
from nodestream.interpreting.interpreter import InterpretationPass


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


# Testing source node creator assignments
TEST_SOURCE_NODE_INTERPRETATION = SourceNodeInterpretation(
    node_type="Test", key={"test_key": "test_value"}
)
TEST_RELATIONSHIP_INTERPRETATION = RelationshipInterpretation(
    node_type="Test", node_key={"test_key": "test_value"}, relationship_type="TEST_REL"
)
TEST_PROPERTIES_INTERPRETATION = PropertiesInterpretation(
    properties={"test_property": "thing"}
)

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

TEST_SWITCH_INTERPRETATION = SwitchInterpretation(
    switch_on="thing",
    cases={
        "case_a": [TEST_SOURCE_NODE_FILE_DATA],
        "case_b": [TEST_SOURCE_NODE_FILE_DATA],
    },
)

SOURCE_NODE_INTERPRETATION_PASS = [
    TEST_SOURCE_NODE_INTERPRETATION,
    TEST_RELATIONSHIP_INTERPRETATION,
    TEST_PROPERTIES_INTERPRETATION,
]

SOURCE_NODE_INTERPRETATION_PASS_ORDERED = [
    TEST_RELATIONSHIP_INTERPRETATION,
    TEST_PROPERTIES_INTERPRETATION,
    TEST_SOURCE_NODE_INTERPRETATION,
]

CONTEXT_INTERPRETATION_PASS = [
    TEST_RELATIONSHIP_INTERPRETATION,
    TEST_PROPERTIES_INTERPRETATION,
]

INTERPRETATION_PASS_WITH_SOURCE_NODE_SWITCH = [
    TEST_SWITCH_INTERPRETATION,
    TEST_RELATIONSHIP_INTERPRETATION,
    TEST_PROPERTIES_INTERPRETATION,
]

INTERPRETATION_PASS_WITH_SOURCE_NODE_SWITCH_ORDERED = [
    TEST_RELATIONSHIP_INTERPRETATION,
    TEST_PROPERTIES_INTERPRETATION,
    TEST_SWITCH_INTERPRETATION,
]


def test_single_sequence_interpretation_pass_assigns_source_nodes():
    subject_a = SingleSequenceInterpretationPass(*SOURCE_NODE_INTERPRETATION_PASS)
    assert subject_a.assigns_source_nodes
    subject_b = SingleSequenceInterpretationPass(*CONTEXT_INTERPRETATION_PASS)
    assert subject_b.assigns_source_nodes is False


def test_multisequence_interpretation_pass_assigns_source_nodes():
    subject_a = MultiSequenceInterpretationPass(
        *[
            SingleSequenceInterpretationPass(*SOURCE_NODE_INTERPRETATION_PASS)
            for _ in range(2)
        ]
    )
    assert subject_a.assigns_source_nodes
    assert subject_a.should_be_distinct

    subject_b = MultiSequenceInterpretationPass(
        *[
            SingleSequenceInterpretationPass(*CONTEXT_INTERPRETATION_PASS)
            for _ in range(2)
        ]
    )
    assert subject_b.assigns_source_nodes is False
    assert subject_b.should_be_distinct is False


def test_null_interpretation_pass_does_not_assign_source_nodes():
    subject = NullInterpretationPass()
    assert subject.assigns_source_nodes is False


def test_interpretation_pass_exclusively_assigns_source_nodes():
    subject_a = SingleSequenceInterpretationPass(*SOURCE_NODE_INTERPRETATION_PASS)
    assert subject_a.exclusively_assigns_source_nodes
    subject_b = SingleSequenceInterpretationPass(*CONTEXT_INTERPRETATION_PASS)
    assert subject_b.exclusively_assigns_source_nodes is False
    subject_c = SingleSequenceInterpretationPass(
        *INTERPRETATION_PASS_WITH_SOURCE_NODE_SWITCH
    )
    assert subject_c.exclusively_assigns_source_nodes is False


def is_ordered(list_a: list, list_b: list):
    if len(list_a) != len(list_b):
        return False
    for i, value in enumerate(list_b):
        if list_a[i] != value:
            return False
    return True


def test_interpretation_pass_interpretation_ordering():
    subject_a = SingleSequenceInterpretationPass(*SOURCE_NODE_INTERPRETATION_PASS)
    assert is_ordered(
        subject_a.schema_ordered_interpretations,
        SOURCE_NODE_INTERPRETATION_PASS_ORDERED,
    )
    subject_b = SingleSequenceInterpretationPass(*CONTEXT_INTERPRETATION_PASS)
    assert is_ordered(
        subject_b.schema_ordered_interpretations, CONTEXT_INTERPRETATION_PASS
    )
    subject_c = SingleSequenceInterpretationPass(
        *INTERPRETATION_PASS_WITH_SOURCE_NODE_SWITCH
    )
    assert is_ordered(
        subject_c.schema_ordered_interpretations,
        INTERPRETATION_PASS_WITH_SOURCE_NODE_SWITCH_ORDERED,
    )


SINGLE_SEQUENCE_MULTIPLE_SOURCE_NODE_GENERATORS = [
    TEST_SOURCE_NODE_FILE_DATA,
    TEST_SOURCE_NODE_FILE_DATA,
]

MULTI_SEQUENCE_INCOMPLETE_SOURCE_NODE_GENERATORS = [
    [
        TEST_RELATIONSHIP_FILE_DATA,
    ],
    [TEST_SOURCE_NODE_FILE_DATA],
]


@pytest.fixture
def environment_flag():
    environ[VALIDATION_FLAG] = "True"


def test_multisequence_interpretation_pass_verifies_source_node_completeness(
    environment_flag,
):
    with pytest.raises(InterpretationPassError) as error:
        _ = InterpretationPass.from_file_data(
            SINGLE_SEQUENCE_MULTIPLE_SOURCE_NODE_GENERATORS
        )
        assert error.message == COMPLETENESS_ERROR_MESSAGE

    with pytest.raises(InterpretationPassError) as error:
        _ = InterpretationPass.from_file_data(
            MULTI_SEQUENCE_INCOMPLETE_SOURCE_NODE_GENERATORS
        )
        assert error.message == UNIQUENESS_ERROR_MESSAGE
