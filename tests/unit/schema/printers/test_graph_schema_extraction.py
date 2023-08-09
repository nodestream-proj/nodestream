from collections import defaultdict
from hamcrest import assert_that, equal_to

from nodestream.schema.printers.graph_schema_extraction import Neo4jLLMGraphSchemaExtraction  

EXPECTED_NODE_PROPS = {'Person': ['name', 'age'], 'Organization': ['name', 'industry']}
EXPECTED_RELS = defaultdict(list, {'Person': defaultdict(list, {'BEST_FRIEND_OF': ['Person']}), 'Organization': defaultdict(list, {'HAS_EMPLOYEE': ['Person']})})
EXPECTED_RELS_PROPS = {'BEST_FRIEND_OF': ['since'], 'HAS_EMPLOYEE': ['since']}

def test_ensure_nodes_props(basic_schema):
    subject = Neo4jLLMGraphSchemaExtraction()
    result = subject.return_nodes_props(basic_schema)
    assert_that(result, equal_to(EXPECTED_NODE_PROPS))

def test_ensure_rels(basic_schema):
    subject = Neo4jLLMGraphSchemaExtraction()
    result = subject.return_rels(basic_schema)
    assert_that(result, equal_to(EXPECTED_RELS))

def test_ensure_rels_props(basic_schema):
    subject = Neo4jLLMGraphSchemaExtraction()
    result = subject.return_rels_props(basic_schema)
    assert_that(result, equal_to(EXPECTED_RELS_PROPS))
