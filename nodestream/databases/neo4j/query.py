from dataclasses import dataclass
from typing import Any, Dict, List

COMMIT_QUERY = """
CALL apoc.periodic.iterate(
    "UNWIND $batched_parameter_sets as params RETURN params",
    $batched_query,
    {batchsize: 1000, parallel: false, retries: 3, params: {batched_parameter_sets: $batched_parameter_sets}}
)
YIELD batches, committedOperations, failedOperations, errorMessages
RETURN batches, committedOperations, failedOperations, errorMessages
"""


@dataclass(slots=True, frozen=True)
class Query:
    query_statement: str
    parameters: Dict[str, Any]

    @classmethod
    def from_statement(cls, statement: str):
        return cls(query_statement=statement, parameters={})


@dataclass(slots=True, frozen=True)
class QueryBatch:
    query_statement: str
    batched_parameter_sets: List[Dict[str, Any]]

    def as_query(self) -> Query:
        return Query(
            COMMIT_QUERY,
            {
                "batched_parameter_sets": self.batched_parameter_sets,
                "batched_query": self.query_statement,
            },
        )
