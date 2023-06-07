from hamcrest import assert_that, equal_to_ignoring_whitespace

from nodestream.cli.schema_printers import GraphQLSchemaPrinter


EXPECTED_GRAPHQL_SCHEMA = """
type Organization @exclude(operations: [CREATE, DELETE, UPDATE]) @pageOptions{limit: {default: 10}}  {
    # Node Properties

    name: String

    industry: String

    # Inbound Relationships

    # Outbound Relationships
 
    
    hasEmployeePerson: [Person!]! 
        @relationship(type: "HAS_EMPLOYEE", direction: OUT, properties: "HasEmployee")
}

type Person @exclude(operations: [CREATE, DELETE, UPDATE]) @pageOptions{limit: {default: 10}}  {
    # Node Properties

    name: String

    age: BigInt

    # Inbound Relationships
 
    
    personBestFriendOf: [Person!]! 
        @relationship(type: "BEST_FRIEND_OF", direction: IN, properties: "BestFriendOf")

    organizationHasEmployee: [Organization!]! 
        @relationship(type: "HAS_EMPLOYEE", direction: IN, properties: "HasEmployee")
    

    # Outbound Relationships
    bestFriendOfPerson: Person 
        @relationship(type: "BEST_FRIEND_OF", direction: OUT, properties: "BestFriendOf")
}
interface BestFriendOf @relationshipProperties { 
    since: LocalDateTime

}
interface HasEmployee @relationshipProperties { 
    since: LocalDateTime

}
"""


def test_outputs_schema_correctly(basic_schema):
    printer = GraphQLSchemaPrinter()
    output = printer.print_schema_to_string(basic_schema)
    assert_that(output, equal_to_ignoring_whitespace(EXPECTED_GRAPHQL_SCHEMA))
