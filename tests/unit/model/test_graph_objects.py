import pytest
from hamcrest import assert_that, equal_to, not_, same_instance

from nodestream.model.graph_objects import (
    Node,
    NodeCreationRule,
    Relationship,
    RelationshipCreationRule,
    RelationshipWithNodes,
    get_cached_timestamp,
)


def test_node_with_no_keys_is_invalid():
    node = Node("Person", {})
    assert_that(node.has_valid_id, equal_to(False))


def test_node_into_ingest():
    node = Node("Person", {"name": "John"})
    ingest = node.into_ingest()
    assert_that(ingest.source, equal_to(node))
    assert_that(ingest.relationships, equal_to([]))


def test_relationship_into_ingest_defaults():
    """into_ingest() respects the creation rule fields on RelationshipWithNodes.

    Default construction uses MATCH_ONLY on both node sides to preserve safe
    semantics for the normal ingest pipeline — endpoint nodes are matched, not
    created. The copy extractor explicitly sets EAGER on both sides when it
    constructs RelationshipWithNodes so that missing destination nodes are created.
    """
    relationship = Relationship("KNOWS", {"since": 2010})
    from_node = Node("Person", {"name": "John"})
    to_node = Node("Person", {"name": "Mary"})
    relationship_with_nodes = RelationshipWithNodes(
        from_node=from_node, to_node=to_node, relationship=relationship
    )
    ingest = relationship_with_nodes.into_ingest()
    assert_that(ingest.source, equal_to(from_node))
    assert_that(ingest.relationships[0].from_node, equal_to(from_node))
    assert_that(ingest.relationships[0].to_node, equal_to(to_node))
    # Defaults: MATCH_ONLY — safe for normal ingest, copy sets EAGER explicitly.
    assert_that(
        ingest.relationships[0].from_side_node_creation_rule,
        equal_to(NodeCreationRule.MATCH_ONLY),
    )
    assert_that(
        ingest.relationships[0].to_side_node_creation_rule,
        equal_to(NodeCreationRule.MATCH_ONLY),
    )
    assert_that(
        ingest.relationships[0].relationship_creation_rule,
        equal_to(RelationshipCreationRule.EAGER),
    )


def test_relationship_into_ingest_match_only():
    """into_ingest() uses MATCH_ONLY when explicitly set — as the copy retriever does."""
    relationship = Relationship("KNOWS", {"since": 2010})
    from_node = Node("Person", {"name": "John"})
    to_node = Node("Person", {"name": "Mary"})
    relationship_with_nodes = RelationshipWithNodes(
        from_node=from_node,
        to_node=to_node,
        relationship=relationship,
        from_side_node_creation_rule=NodeCreationRule.MATCH_ONLY,
        to_side_node_creation_rule=NodeCreationRule.MATCH_ONLY,
    )
    ingest = relationship_with_nodes.into_ingest()
    assert_that(
        ingest.relationships[0].from_side_node_creation_rule,
        equal_to(NodeCreationRule.MATCH_ONLY),
    )
    assert_that(
        ingest.relationships[0].to_side_node_creation_rule,
        equal_to(NodeCreationRule.MATCH_ONLY),
    )


@pytest.mark.parametrize(
    "keys,expected",
    [
        ({"name": "John"}, True),
        ({"name": None}, False),
        ({"name": "John", "age": 30}, True),
        ({"name": "John", "age": None}, False),
        ({"name": None, "age": None}, False),
    ],
)
def test_node_key_validity(keys, expected):
    node = Node("Person", keys)
    assert_that(node.has_valid_id, equal_to(expected))


def test_get_cached_timestamp():
    # The first and second timestamp should be the same if called in quick
    # succession.
    t = 10
    first = get_cached_timestamp(epoch=t)
    a_little_after = get_cached_timestamp(epoch=t + 0.00001)
    a_little_before = get_cached_timestamp(epoch=t - 0.00001)
    assert_that(first, equal_to(a_little_before))
    assert_that(first, equal_to(a_little_after))

    third = get_cached_timestamp(epoch=t + 2.1)
    assert_that(third, not_(same_instance(first)))


@pytest.mark.parametrize(
    "keys,expected",
    [
        (
            {"name": "John"},
            (("name", "John"),),
        ),
        ({"name": "John", "age": 30}, (("age", 30), ("name", "John"))),
        (
            {"name": "John", "age": 30, "city": "New York"},
            (("age", 30), ("city", "New York"), ("name", "John")),
        ),
    ],
)
def test_get_dedup_key(keys, expected):
    node = Node("Person", keys)
    assert_that(node.get_dedup_key(), equal_to(expected))

    other = Node("Person", keys)
    assert_that(node.get_dedup_key(), equal_to(other.get_dedup_key()))


def test_relationship_get_dedup_key_includes_property_names():
    # Two relationships with different key property names but the same value
    # must produce different dedup keys so they are not falsely collapsed
    # within a batch. Previously get_dedup_key() used .values() only, meaning
    # {reason: 'foo'} and {source: 'foo'} would collide on ('foo',).
    rel_reason = Relationship("KNOWS", {"reason": "foo"})
    rel_source = Relationship("KNOWS", {"source": "foo"})
    assert_that(rel_reason.get_dedup_key(), not_(equal_to(rel_source.get_dedup_key())))


def test_relationship_get_dedup_key_same_keys_and_values_are_equal():
    rel_a = Relationship("KNOWS", {"reason": "foo"})
    rel_b = Relationship("KNOWS", {"reason": "foo"})
    assert_that(rel_a.get_dedup_key(), equal_to(rel_b.get_dedup_key()))
