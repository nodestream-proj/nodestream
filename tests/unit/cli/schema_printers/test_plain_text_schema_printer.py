from hamcrest import assert_that, equal_to_ignoring_whitespace

from nodestream.cli.schema_printers import PlainTestSchemaPrinter

EXPECTED_OUTPUT = """
[NODE] Person:
    name: STRING
    age: INTEGER
[NODE] Organization:
    name: STRING
    industry: STRING
[RELATIONSHIP] BEST_FRIEND_OF:
    since: DATETIME
[RELATIONSHIP] HAS_EMPLOYEE:
    since: DATETIME
"""


def test_outputs_schema_correctly(basic_schema):
    printer = PlainTestSchemaPrinter()
    output = printer.print_schema_to_string(basic_schema)
    assert_that(output, equal_to_ignoring_whitespace(EXPECTED_OUTPUT))
