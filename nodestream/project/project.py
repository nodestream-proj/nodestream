from pathlib import Path
from typing import Dict, List, Optional

from yaml import safe_load

from .pipeline_scope import PipelineScope
from .run_request import RunRequest
from ..exceptions import MissingProjectFileError


DEFAULT_PROJECT_FILE = Path("nodestream.yaml")


class Project:
    @classmethod
    def from_file(cls, path: Optional[Path]) -> "Project":
        path = path or DEFAULT_PROJECT_FILE
        if not path.exists:
            raise MissingProjectFileError(path)

        with open(path) as fp:
            return cls.from_file_data(safe_load(fp))

    @classmethod
    def from_file_data(cls, data) -> "Project":
        scopes_data = data.pop("scopes", {})
        scopes = [
            PipelineScope.from_file_data(*scope_data)
            for scope_data in scopes_data.items()
        ]
        return cls(scopes)

    def __init__(self, scopes: List[PipelineScope]):
        self.scopes_by_name: Dict[str, PipelineScope] = {
            scope.name: scope for scope in scopes
        }

    async def run(self, request: RunRequest):
        for scope in self.scopes_by_name.values():
            await scope.run_request(request)
