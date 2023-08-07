import json
from abc import ABC, abstractmethod
from contextlib import contextmanager
from csv import DictReader
from glob import glob
from io import StringIO
from pathlib import Path
from typing import Any, AsyncGenerator, Iterable, Union

from ...model import JsonLikeDocument
from ...pluggable import Pluggable
from ...subclass_registry import SubclassRegistry
from .extractor import Extractor

SUPPORTED_FILE_FORMAT_REGISTRY = SubclassRegistry()


@SUPPORTED_FILE_FORMAT_REGISTRY.connect_baseclass
class SupportedFileFormat(Pluggable, ABC):
    def __init__(self, file: Union[Path, StringIO]) -> None:
        self.file = file

    @contextmanager
    def read_handle(self) -> StringIO:
        if isinstance(self.file, Path):
            with open(self.file, "r") as fp:
                yield fp
        else:
            yield self.file

    def read_file(self) -> Iterable[JsonLikeDocument]:
        with self.read_handle() as fp:
            return self.read_file_from_handle(fp)

    @classmethod
    @contextmanager
    def open(cls, file: Path) -> "SupportedFileFormat":
        with open(file, "r") as fp:
            yield cls.from_file_pointer_and_format(fp, file.suffix)

    @classmethod
    def from_file_pointer_and_format(
        cls, fp: StringIO, file_format: str
    ) -> "SupportedFileFormat":
        # Import all file formats so that they can register themselves
        cls.import_all()
        file_format = SUPPORTED_FILE_FORMAT_REGISTRY.get(file_format)
        return file_format(fp)

    @abstractmethod
    def read_file_from_handle(self, fp: StringIO) -> Iterable[JsonLikeDocument]:
        ...


class JsonFileFormat(SupportedFileFormat, alias=".json"):
    def read_file_from_handle(self, fp: StringIO) -> Iterable[JsonLikeDocument]:
        return [json.load(fp)]


class TextFileFormat(SupportedFileFormat, alias=".txt"):
    def read_file_from_handle(self, fp: StringIO) -> Iterable[JsonLikeDocument]:
        return ({"line": line} for line in fp)


class CommaSeperatedValuesFileFormat(SupportedFileFormat, alias=".csv"):
    def read_file_from_handle(self, fp: StringIO) -> Iterable[JsonLikeDocument]:
        return DictReader(fp)


class FileExtractor(Extractor):
    @classmethod
    def from_file_data(cls, globs: Iterable[str]):
        all_matching_paths = (
            Path(file)
            for glob_string in globs
            for file in glob(glob_string, recursive=True)
        )
        final_paths = {p for p in all_matching_paths if p.is_file()}
        return cls(final_paths)

    def __init__(self, paths: Iterable[Path]) -> None:
        self.paths = paths

    async def extract_records(self) -> AsyncGenerator[Any, Any]:
        for path in self.paths:
            with SupportedFileFormat.open(path) as file:
                for record in file.read_file():
                    yield record
