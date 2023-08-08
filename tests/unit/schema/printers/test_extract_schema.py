from collections import defaultdict
from nodestream.generators.schemas.Neo4jLLMGraphSchemaExtraction import Neo4jLLMGraphSchemaExtraction  

from nodestream.schema import (
    Cardinality,
    GraphObjectShape,
    GraphObjectType,
    GraphSchema,
    KnownTypeMarker,
    PresentRelationship,
    PropertyMetadata,
    PropertyMetadataSet,
    PropertyType,
)

test_schema = GraphSchema(
        [
            GraphObjectShape(
                GraphObjectType.NODE,
                KnownTypeMarker("Person"),
                PropertyMetadataSet(
                    {
                        "name": PropertyMetadata("name", PropertyType.STRING),
                        "age": PropertyMetadata("age", PropertyType.INTEGER),
                    }
                ),
            ),
            GraphObjectShape(
                GraphObjectType.NODE,
                KnownTypeMarker("Organization"),
                PropertyMetadataSet(
                    {
                        "name": PropertyMetadata("name", PropertyType.STRING),
                        "industry": PropertyMetadata("industry", PropertyType.STRING),
                    }
                ),
            ),
            GraphObjectShape(
                GraphObjectType.RELATIONSHIP,
                KnownTypeMarker("BEST_FRIEND_OF"),
                PropertyMetadataSet(
                    {
                        "since": PropertyMetadata("since", PropertyType.DATETIME),
                    }
                ),
            ),
            GraphObjectShape(
                GraphObjectType.RELATIONSHIP,
                KnownTypeMarker("HAS_EMPLOYEE"),
                PropertyMetadataSet(
                    {
                        "since": PropertyMetadata("since", PropertyType.DATETIME),
                    }
                ),
            ),
        ],
        [
            PresentRelationship(
                KnownTypeMarker("Person"),
                KnownTypeMarker("Person"),
                KnownTypeMarker("BEST_FRIEND_OF"),
                from_side_cardinality=Cardinality.SINGLE,
                to_side_cardinality=Cardinality.MANY,
            ),
            PresentRelationship(
                KnownTypeMarker("Organization"),
                KnownTypeMarker("Person"),
                KnownTypeMarker("HAS_EMPLOYEE"),
                from_side_cardinality=Cardinality.MANY,
                to_side_cardinality=Cardinality.MANY,
            ),
        ],
    )

EXPECTED_NODE_PROPS = {'Person': ['name', 'age'], 'Organization': ['name', 'industry']}

EXPECTED_RELS = defaultdict(list, {'Person': defaultdict(list, {'BEST_FRIEND_OF': ['Person']}), 'Organization': defaultdict(list, {'HAS_EMPLOYEE': ['Person']})})

EXPECTED_RELS_PROPS = {'BEST_FRIEND_OF': ['since'], 'HAS_EMPLOYEE': ['since']}


def test_ensure_nodes_props():
    
    subject = Neo4jLLMGraphSchemaExtraction(test_schema)
    result = subject.return_nodes_props(test_schema)
    assert result == EXPECTED_NODE_PROPS


def test_ensure_rels():
    
    subject = Neo4jLLMGraphSchemaExtraction(test_schema)
    result = subject.return_rels(test_schema)
    assert result == EXPECTED_RELS

def test_ensure_rels_props():
    
    subject = Neo4jLLMGraphSchemaExtraction(test_schema)
    result = subject.return_rels_props(test_schema)
    assert result == EXPECTED_RELS_PROPS
