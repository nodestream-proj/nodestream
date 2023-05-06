from hamcrest import assert_that, has_entry, equal_to

from nodestream.interpretations.extract_variables_interpretation import (
    ExtractVariablesInterpretation,
)

from ..stubs import StubbedValueProvider


def test_extract_variables_without_normalization(blank_context):
    expected_value = input_value = "thisIsATest"
    subject = ExtractVariablesInterpretation(
        variables={"test": StubbedValueProvider(values=[input_value])}
    )
    subject.interpret(blank_context)
    assert_that(
        blank_context.variables, has_entry(equal_to("test"), equal_to(expected_value))
    )


def test_extract_variables_with_normalization(blank_context):
    input_value = "thisIsATest"
    expected_value = "thisisatest"
    subject = ExtractVariablesInterpretation(
        variables={"test": StubbedValueProvider(values=[input_value])},
        normalization={"do_lowercase_strings": True},
    )
    subject.interpret(blank_context)
    assert_that(
        blank_context.variables, has_entry(equal_to("test"), equal_to(expected_value))
    )
