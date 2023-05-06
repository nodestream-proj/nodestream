from hamcrest import assert_that, equal_to, instance_of

from nodestream.interpretations import Interpretation, SourceNodeInterpretation


def test_from_file_arguments_gets_right_subclass():
    result = Interpretation.from_file_arguments(
        type="source_node", node_type="Test", key={"key": "value"}
    )
    assert_that(result, instance_of(SourceNodeInterpretation))
