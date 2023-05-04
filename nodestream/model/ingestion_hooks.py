from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Tuple


class IngestionHook(ABC):
    """An IngestionHook is a custom piece of logic that is bundled as a query.

    IngestionHooks provide a mechanism to enrich the graph model that is derived from
    data added to and currently existing in the graph. For example, drawing an edge
    between two nodes where following a complex path.
    """

    @abstractmethod
    def as_cypher_query_and_parameters(self) -> Tuple[str, Dict[str, Any]]:
        """Returns a cypher query string and parameters to execute."""

        raise NotImplementedError


@dataclass(slots=True, frozen=True)
class IngestionHookRunRequest:
    """An `IngestionHookRunRequest` defines what hook is meant to be run and when the hook should run."""

    hook: IngestionHook
    before_ingest: bool
