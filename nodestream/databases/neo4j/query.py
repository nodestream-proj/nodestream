from dataclasses import dataclass
from typing import Any, Dict, List

UNWIND_QUERY = "UNWIND $batched_parameter_sets as params RETURN params"
COMMIT_QUERY = """
CALL apoc.periodic.iterate(
    $iterable_query,
    $batched_query,
    {batchsize: 1000, parallel: true, retries: 3, params: $iterate_params}
)
YIELD batches, committedOperations, failedOperations, errorMessages
RETURN batches, committedOperations, failedOperations, errorMessages
"""

NON_APOCH_COMMIT_QUERY = """
UNWIND $iterate_params.batched_parameter_sets AS param
CALL apoc.cypher.doIt($batched_query, {params: param})
YIELD value
RETURN value
"""


@dataclass(slots=True, frozen=True)
class Query:
    query_statement: str
    parameters: Dict[str, Any]

    @classmethod
    def from_statement(cls, statement: str):
        return cls(query_statement=statement, parameters={})

    def feed_batched_query(self, batched_query: str) -> "Query":
        """Feed the results of the the query into another query that will be executed in batches."""
        return Query(
            COMMIT_QUERY,
            {
                "iterate_params": self.parameters,
                "batched_query": batched_query,
                "iterable_query": self.query_statement,
            },
        )


@dataclass(slots=True, frozen=True)
class QueryBatch:
    query_statement: str
    batched_parameter_sets: List[Dict[str, Any]]

    def as_query(self, apoc_iterate: bool) -> Query:
        return Query(
            {True: COMMIT_QUERY, False: NON_APOCH_COMMIT_QUERY}[apoc_iterate],
            {
                "iterate_params": {
                    "batched_parameter_sets": self.batched_parameter_sets
                },
                "batched_query": self.query_statement,
                "iterable_query": UNWIND_QUERY,
            },
        )
