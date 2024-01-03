from yaml import load

from .argument_resolver import ArgumentResolver


class IncludeFileResolver(ArgumentResolver, alias="include"):
    """An `IncludeFileResolver` is an `ArgumentResolver` that can resolve a file path into a file's contents."""

    @staticmethod
    def resolve_argument(file_path: str):
        from ..pipeline_file_loader import PipelineFileContents

        with open(file_path) as f:
            return load(f, Loader=PipelineFileContents.get_loader())
