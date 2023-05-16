from dataclasses import dataclass
from pathlib import Path

from ..pipeline import Pipeline, PipelineFileLoader, PipelineInitializationArguments


@dataclass
class PipelineDefinition:
    name: str
    file_path: Path

    def initialize(self, init_args: PipelineInitializationArguments) -> Pipeline:
        return PipelineFileLoader(self.file_path).load_pipeline(init_args)
