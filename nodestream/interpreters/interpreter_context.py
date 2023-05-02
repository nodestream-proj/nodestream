from dataclasses import dataclass, field
from typing import Dict, Optional, Any

from ..model import JsonDocument
from ..model.desired_ingest import DesiredIngestion

# TODO: Better docs in this file.

@dataclass(slots=True)
class InterpreterContext:
    """ Defines the context of an interpretation. """
    document: JsonDocument
    desired_ingest: DesiredIngestion
    mappings: Optional[Dict[Any, Any]] = None
    variables: Optional[Dict[str, Any]] = field(default_factory=dict)
