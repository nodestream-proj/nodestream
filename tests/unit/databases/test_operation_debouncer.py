from nodestream.model import Node, Relationship, RelationshipWithNodes, MatchStrategy
from nodestream.databases.operation_debouncer import OperationDebouncer

from hamcrest import assert_that, equal_to, has_length


def test_debounces_updates_to_nodes_with_same_key():
    debouncer = OperationDebouncer()
    node1 = Node("NodeType", {"id": "1", "name": "foo"}, {"foo": "bar"})
    node2 = Node("NodeType", {"id": "1", "name": "foo"}, {"foo": "baz"})
    debouncer.debounce_node_operation(node1)
    debouncer.debounce_node_operation(node2)

    result = list(debouncer.drain_node_groups())
    assert len(result) == 1
    assert len(result[0][1]) == 1


def test_debounces_updates_to_nodes_with_same_key_mixed_input():
    debouncer = OperationDebouncer()
    node1 = Node("NodeType", {"id": "1", "name": "foo"}, {"foo": "bar"})
    node2 = Node("FooType", {"id": "1", "name": "foo"}, {"foo": "qux"})
    node3 = Node("NodeType", {"id": "1", "name": "foo"}, {"foo": "baz"})

    debouncer.debounce_node_operation(node1)
    debouncer.debounce_node_operation(node2)
    debouncer.debounce_node_operation(node3)

    result = list(debouncer.drain_node_groups())

    assert_that(result, has_length(2))
    assert_that(result[0][1], has_length(1))
    assert_that(result[1][1], has_length(1))


def test_debounces_updates_to_nodes_with_same_key_same_type_different_keys():
    debouncer = OperationDebouncer()
    node1 = Node("NodeType", {"id": "1", "name": "foo"}, {"foo": "bar"})
    node2 = Node("NodeType", {"id": "2", "name": "bar"}, {"foo": "qux"})
    node3 = Node("NodeType", {"id": "1", "name": "foo"}, {"foo": "baz"})

    debouncer.debounce_node_operation(node1)
    debouncer.debounce_node_operation(node2)
    debouncer.debounce_node_operation(node3)

    result = list(debouncer.drain_node_groups())

    assert_that(result, has_length(1))
    assert_that(result[0][1], has_length(2))


def test_debounced_updates_to_relationships_with_same_nodes_and_keys():
    debouncer = OperationDebouncer()
    rel1 = RelationshipWithNodes(
        to_node=Node("NodeType", {"id": "1", "name": "foo"}, {"foo": "node_one_val1"}),
        from_node=Node(
            "NodeType", {"id": "2", "name": "bar"}, {"foo": "node_two_val1"}
        ),
        relationship=Relationship("REL_TYPE", {"foo": "bar"}, {"prop": "rel_val1"}),
    )
    rel2 = RelationshipWithNodes(
        to_node=Node("NodeType", {"id": "1", "name": "foo"}, {"foo": "node_one_val2"}),
        from_node=Node(
            "NodeType", {"id": "2", "name": "bar"}, {"foo": "node_two_val2"}
        ),
        relationship=Relationship("REL_TYPE", {"foo": "bar"}, {"prop": "rel_val2"}),
    )
    debouncer.debounce_relationship(rel1)
    debouncer.debounce_relationship(rel2)

    result = list(debouncer.drain_relationship_groups())

    assert_that(result, has_length(1))
    assert_that(result[0][1], has_length(1))


def test_debounces_nodes_with_different_match_strategies():
    debouncer = OperationDebouncer()
    node1 = Node("NodeType", {"id": "1", "name": "foo"}, {"foo": "bar"})
    node2 = Node("NodeType", {"id": "1", "name": "foo"}, {"foo": "baz"})
    debouncer.debounce_node_operation(node1, match_strategy=MatchStrategy.EAGER)
    debouncer.debounce_node_operation(node2, match_strategy=MatchStrategy.FUZZY)

    result = list(debouncer.drain_node_groups())
    assert len(result) == 2
    assert len(result[0][1]) == 1
    assert len(result[1][1]) == 1


def test_debounced_relationships_with_different_match_strategies():
    debouncer = OperationDebouncer()
    rel1 = RelationshipWithNodes(
        to_node=Node("NodeType", {"id": "1", "name": "foo"}, {"foo": "node_one_val1"}),
        from_node=Node(
            "NodeType", {"id": "2", "name": "bar"}, {"foo": "node_two_val1"}
        ),
        relationship=Relationship("REL_TYPE", {"foo": "bar"}, {"prop": "rel_val1"}),
        from_side_match_strategy=MatchStrategy.FUZZY,
    )
    rel2 = RelationshipWithNodes(
        to_node=Node("NodeType", {"id": "1", "name": "foo"}, {"foo": "node_one_val2"}),
        from_node=Node(
            "NodeType", {"id": "2", "name": "bar"}, {"foo": "node_two_val2"}
        ),
        relationship=Relationship("REL_TYPE", {"foo": "bar"}, {"prop": "rel_val2"}),
        from_side_match_strategy=MatchStrategy.MATCH_ONLY,
    )
    debouncer.debounce_relationship(rel1)
    debouncer.debounce_relationship(rel2)

    result = list(debouncer.drain_relationship_groups())

    assert_that(result, has_length(2))
    assert_that(result[0][1], has_length(1))
    assert_that(result[1][1], has_length(1))


def test_debounced_relationships_with_different_match_strategies_eager_does_not_dup():
    debouncer = OperationDebouncer()
    rel1 = RelationshipWithNodes(
        to_node=Node("NodeType", {"id": "1", "name": "foo"}, {"foo": "node_one_val1"}),
        from_node=Node(
            "NodeType", {"id": "2", "name": "bar"}, {"foo": "node_two_val1"}
        ),
        relationship=Relationship("REL_TYPE", {"foo": "bar"}, {"prop": "rel_val1"}),
        from_side_match_strategy=MatchStrategy.EAGER,
    )
    rel2 = RelationshipWithNodes(
        to_node=Node("NodeType", {"id": "1", "name": "foo"}, {"foo": "node_one_val2"}),
        from_node=Node(
            "NodeType", {"id": "2", "name": "bar"}, {"foo": "node_two_val2"}
        ),
        relationship=Relationship("REL_TYPE", {"foo": "bar"}, {"prop": "rel_val2"}),
        from_side_match_strategy=MatchStrategy.MATCH_ONLY,
    )
    debouncer.debounce_relationship(rel1)
    debouncer.debounce_relationship(rel2)

    result = list(debouncer.drain_relationship_groups())

    assert_that(result, has_length(1))
    assert_that(result[0][1], has_length(1))
