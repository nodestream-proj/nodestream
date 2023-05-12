from dataclasses import dataclass
from typing import Dict, Any, List


@dataclass(slots=True)
class Query:
    query_statement: str
    parameters: Dict[str, Any]

    @classmethod
    def from_statement(cls, statement: str):
        return cls(query_statement=statement, parameters={})


@dataclass(slots=True)
class QueryBatch:
    query_statement: str
    batched_parameters: List[Dict[str, Any]]
