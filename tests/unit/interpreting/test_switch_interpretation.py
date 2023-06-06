import pytest
from hamcrest import assert_that, has_entry, not_

from nodestream.interpreting.switch_interpretation import (
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
