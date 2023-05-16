from dataclasses import dataclass
from typing import Dict, List

from .pipeline_scope import PipelineScope
from .run_request import RunRequest


class Project:
    def __init__(self, scopes: List[PipelineScope]):
        self.scopes_by_name: Dict[str, PipelineScope] = {
            scope.name: scope for scope in scopes
        }

    async def run(self, request: RunRequest):
        for scope in self.scopes_by_name.values():
            await scope.run_request(request)
