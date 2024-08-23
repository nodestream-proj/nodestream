from hamcrest import assert_that, equal_to

from nodestream.schema.printers.cypheresque import CypherEsquePrinter


def test_print_schema_to_string(basic_schema):
    printer = CypherEsquePrinter()
    result = printer.print_schema_to_string(basic_schema)

    assert_that(
        result,
        equal_to(
            """Node Types:
Person: name: STRING, age: INTEGER
Organization: name: STRING, industry: STRING
Relationship Types:
BEST_FRIEND_OF: since: DATETIME
HAS_EMPLOYEE: since: DATETIME
Adjancies:
(:Person)-[:BEST_FRIEND_OF]->(:Person)
(:Organization)-[:HAS_EMPLOYEE]->(:Person)
"""
        ),
    )
