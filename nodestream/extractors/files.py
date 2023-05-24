import json
from abc import ABC, abstractmethod
from contextlib import contextmanager
from csv import DictReader
from glob import glob
from io import StringIO
from pathlib import Path
from typing import Any, AsyncGenerator, Iterable, Union

from ..model import JsonLikeDocument
from ..pipeline import Extractor
from ..subclass_registry import SubclassRegistry

SUPPORTED_FILE_FORMAT_REGISTRY = SubclassRegistry()


@SUPPORTED_FILE_FORMAT_REGISTRY.connect_baseclass
class SupportedFileFormat(ABC):
    def __init__(self, file: Union[Path, StringIO]) -> None:
        self.file = file

    @contextmanager
    def read_handle(self) -> StringIO:
        if isinstance(self.file, Path):
            with open(self.file, "r+") as fp:
                yield fp
        else:
            yield self.file

    def read_file(self) -> Iterable[JsonLikeDocument]:
        with self.read_handle() as fp:
            return self.read_file_from_handle(fp)

    @classmethod
    def open(cls, file: Path) -> "SupportedFileFormat":
        return cls.from_file_pointer_and_format(file, file.suffix)

    @classmethod
    def from_file_pointer_and_format(
        cls, fp: StringIO, file_format: str
    ) -> "SupportedFileFormat":
        file_format = SUPPORTED_FILE_FORMAT_REGISTRY.get(file_format)
        return file_format(fp)

    @abstractmethod
    def read_file_from_handle(self, fp: StringIO) -> Iterable[JsonLikeDocument]:
        ...


class JsonFileFormat(SupportedFileFormat, name=".json"):
    def read_file_from_handle(self, fp: StringIO) -> Iterable[JsonLikeDocument]:
        return [json.load(fp)]


class TextFileFormat(SupportedFileFormat, name=".txt"):
    def read_file_from_handle(self, fp: StringIO) -> Iterable[JsonLikeDocument]:
        return [{"line": line} for line in fp.readlines()]


class CommaSeperatedValuesFileFormat(SupportedFileFormat, name=".csv"):
    def read_file_from_handle(self, fp: StringIO) -> Iterable[JsonLikeDocument]:
        return list(DictReader(fp))


class FileExtractor(Extractor):
    @classmethod
    def __declarative_init__(cls, globs: Iterable[str]):
        paths = (file for glob_string in globs for file in glob(glob_string))
        return cls(paths)

    def __init__(self, paths: Iterable[Iterable]) -> None:
        self.paths = paths

    async def extract_records(self) -> AsyncGenerator[Any, Any]:
        for path in self.paths:
            for record in SupportedFileFormat.open(path).read_file():
                yield record