import bz2
import gzip
import json
import os
import tempfile
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager, contextmanager
from csv import DictReader
from glob import glob
from io import BufferedReader, BytesIO, IOBase, StringIO, TextIOWrapper
from pathlib import Path
from typing import Any, AsyncGenerator, Callable, Generator, Iterable

import pandas as pd
from httpx import AsyncClient
from yaml import safe_load

from ...model import JsonLikeDocument
from ...pluggable import Pluggable
from ...subclass_registry import SubclassRegistry
from .extractor import Extractor

SUPPORTED_FILE_FORMAT_REGISTRY = SubclassRegistry()
SUPPORTED_COMPRESSED_FILE_FORMAT_REGISTRY = SubclassRegistry()


class IngestibleFile:
    def __init__(
        self,
        path: Path,
        fp: IOBase | None = None,
        delete_on_ingestion: bool = False,
        on_ingestion: Callable[[Any], Any] = lambda: (),
    ) -> None:
        self.extension = path.suffix
        self.suffixes = path.suffixes
        self.fp = fp
        self.path = path
        self.delete_on_ingestion = delete_on_ingestion
        self.on_ingestion = on_ingestion

    @classmethod
    def from_file_pointer_and_suffixes(
        cls,
        fp: IOBase,
        suffixes: str | list[str],
        on_ingestion: Callable[[Any], Any] = lambda: (),
    ) -> "IngestibleFile":
        fd, temp_path = tempfile.mkstemp(suffix="".join(suffixes))
        os.close(fd)

        with open(temp_path, "wb") as temp_file:
            for chunk in iter(lambda: fp.read(1024), b""):
                temp_file.write(chunk)
            temp_file.flush()

        with open(temp_path, "rb+") as fp:
            return IngestibleFile(Path(temp_path), fp, True, on_ingestion)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.fp:
            self.fp.close()
        if self.delete_on_ingestion:
            self.tempfile_cleanup()

    def ingested(self):
        if self.delete_on_ingestion:
            self.tempfile_cleanup()
        self.on_ingestion()

    def tempfile_cleanup(self):
        if self.fp:
            self.fp.close()
        if os.path.isfile(self.path):
            os.remove(self.path)


@SUPPORTED_FILE_FORMAT_REGISTRY.connect_baseclass
class SupportedFileFormat(Pluggable, ABC):
    reader = None

    def __init__(self, file: IngestibleFile) -> None:
        self.file = file

    @contextmanager
    def read_handle(self) -> Iterable | BufferedReader:
        if isinstance(self.file, Path):
            with open(self.file, "rb") as fp:
                yield fp
        else:
            yield self.file

    def read_file(self) -> Iterable[JsonLikeDocument]:
        with self.read_handle() as fp:
            if self.reader is not None:
                if self.reader is TextIOWrapper:
                    reader = self.reader(fp, encoding="utf-8")
                else:
                    reader = self.reader(fp)

            else:
                reader = fp
            return self.read_file_from_handle(reader)

    @classmethod
    @contextmanager
    def open(cls, file: IngestibleFile) -> Generator["SupportedFileFormat", None, None]:
        extension = file.extension
        # Decompress file if in Supported Compressed File Format Registry
        while extension in SUPPORTED_COMPRESSED_FILE_FORMAT_REGISTRY:
            compressed_file_format = SupportedCompressedFileFormat.open(file)
            file = compressed_file_format.decompress_file()
            extension = file.extension
        with open(file.path, "rb") as fp:
            yield cls.from_file_pointer_and_format(fp, extension)

    @classmethod
    def from_file_pointer_and_format(
        cls, fp: IOBase, file_format: str
    ) -> "SupportedFileFormat":
        # Import all file formats so that they can register themselves
        cls.import_all()
        file_format = SUPPORTED_FILE_FORMAT_REGISTRY.get(file_format)
        return file_format(fp)

    @abstractmethod
    def read_file_from_handle(
        self, fp: BufferedReader
    ) -> Iterable[JsonLikeDocument]: ...


@SUPPORTED_COMPRESSED_FILE_FORMAT_REGISTRY.connect_baseclass
class SupportedCompressedFileFormat(Pluggable, ABC):
    def __init__(self, file: IngestibleFile) -> None:
        self.file = file

    @classmethod
    def open(cls, file: IngestibleFile) -> "SupportedCompressedFileFormat":
        with open(file.path, "rb") as fp:
            return cls.from_file_pointer_and_path(fp, file.path)

    @classmethod
    def from_file_pointer_and_path(
        cls, fp: IOBase, path: Path
    ) -> "SupportedCompressedFileFormat":
        # Import all compression file formats so that they can register themselves
        cls.import_all()
        file_format = SUPPORTED_COMPRESSED_FILE_FORMAT_REGISTRY.get(path.suffix)
        file = IngestibleFile(path, fp)
        file.on_ingestion = lambda: file.tempfile_cleanup()
        return file_format(file)

    @abstractmethod
    def decompress_file(self) -> IngestibleFile: ...


class JsonFileFormat(SupportedFileFormat, alias=".json"):
    reader = TextIOWrapper

    def read_file_from_handle(
        self, reader: TextIOWrapper
    ) -> Iterable[JsonLikeDocument]:
        return [json.load(reader)]


class LineSeperatedJsonFileFormat(SupportedFileFormat, alias=".jsonl"):
    reader = TextIOWrapper

    def read_file_from_handle(
        self, reader: TextIOWrapper
    ) -> Iterable[JsonLikeDocument]:
        return (json.loads(line.strip()) for line in reader.readlines())


class ParquetFileFormat(SupportedFileFormat, alias=".parquet"):
    def read_file_from_handle(self, fp: StringIO) -> Iterable[JsonLikeDocument]:
        df = pd.read_parquet(fp, engine="pyarrow")
        return (row[1].to_dict() for row in df.iterrows())


class TextFileFormat(SupportedFileFormat, alias=".txt"):
    reader = TextIOWrapper

    def read_file_from_handle(
        self, reader: TextIOWrapper
    ) -> Iterable[JsonLikeDocument]:
        return ({"line": line.strip()} for line in reader.readlines())


class CommaSeperatedValuesFileFormat(SupportedFileFormat, alias=".csv"):
    reader = TextIOWrapper

    def read_file_from_handle(
        self, reader: TextIOWrapper
    ) -> Iterable[JsonLikeDocument]:
        return DictReader(reader)


class YamlFileFormat(SupportedFileFormat, alias=".yaml"):
    reader = TextIOWrapper

    def read_file_from_handle(
        self, reader: TextIOWrapper
    ) -> Iterable[JsonLikeDocument]:
        return [safe_load(reader)]


class GzipFileFormat(SupportedCompressedFileFormat, alias=".gz"):
    def decompress_file(self) -> IngestibleFile:
        decompressed_data = BytesIO()
        with gzip.open(self.file.path, "rb") as f_in:
            chunk_size = 1024 * 1024
            while True:
                chunk = f_in.read(chunk_size)
                if len(chunk) == 0:
                    break
                decompressed_data.write(chunk)
        decompressed_data.seek(0)
        new_path = self.file.path.with_suffix("")
        temp_file = IngestibleFile.from_file_pointer_and_suffixes(
            decompressed_data, new_path.suffixes
        )
        self.file.ingested()
        return temp_file


class Bz2FileFormat(SupportedCompressedFileFormat, alias=".bz2"):
    def decompress_file(self) -> IngestibleFile:
        decompressed_data = BytesIO()
        with bz2.open(self.file.path, "rb") as f_in:
            chunk_size = 1024 * 1024
            while True:
                chunk = f_in.read(chunk_size)
                if len(chunk) == 0:
                    break
                decompressed_data.write(chunk)
        decompressed_data.seek(0)
        new_path = self.file.path.with_suffix("")
        temp_file = IngestibleFile.from_file_pointer_and_suffixes(
            decompressed_data, new_path.suffixes
        )
        self.file.ingested()
        return temp_file


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

    def _ordered_paths(self) -> Iterable[Path]:
        return sorted(self.paths)

    async def extract_records(self) -> AsyncGenerator[Any, Any]:
        for path in self._ordered_paths():
            with SupportedFileFormat.open(IngestibleFile(path)) as file:
                for record in file.read_file():
                    yield record


class RemoteFileExtractor(Extractor):
    def __init__(
        self, urls: Iterable[str], memory_spooling_max_size_in_mb: int = 5
    ) -> None:
        self.urls = urls
        self.memory_spooling_max_size = memory_spooling_max_size_in_mb * 1024 * 1024

    @asynccontextmanager
    async def download_file(self, client: AsyncClient, url: str) -> SupportedFileFormat:
        with tempfile.TemporaryFile() as fp:
            async with client.stream("GET", url) as response:
                async for chunk in response.aiter_bytes():
                    fp.write(chunk)
            fp.seek(0)
            yield SupportedFileFormat.from_file_pointer_and_format(fp, Path(url).suffix)

    async def extract_records(self) -> AsyncGenerator[Any, Any]:
        async with AsyncClient() as client:
            for url in self.urls:
                async with self.download_file(client, url) as file:
                    for record in file.read_file():
                        yield record
