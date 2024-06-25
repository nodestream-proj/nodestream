import bz2
import gzip
import json
import tempfile
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager, contextmanager
from csv import DictReader
from glob import glob
from io import BufferedReader, BytesIO, IOBase, StringIO, TextIOWrapper
from pathlib import Path
from typing import Any, AsyncGenerator, Iterable, Union

import pandas as pd
from httpx import AsyncClient
from yaml import safe_load

from ...model import JsonLikeDocument
from ...pluggable import Pluggable
from ...subclass_registry import SubclassRegistry
from .extractor import Extractor

SUPPORTED_FILE_FORMAT_REGISTRY = SubclassRegistry()
SUPPORTED_COMPRESSED_FILE_FORMAT_REGISTRY = SubclassRegistry()


@SUPPORTED_FILE_FORMAT_REGISTRY.connect_baseclass
class SupportedFileFormat(Pluggable, ABC):
    reader = None

    def __init__(self, file: Union[Path, IOBase]) -> None:
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
                if self.reader is TextIOWrapper or BytesIO:
                    reader = self.reader(fp, encoding="utf-8")
                else:
                    reader = self.reader(fp)

            else:
                reader = fp
            return self.read_file_from_handle(reader)

    @classmethod
    @contextmanager
    def open(cls, file: Path) -> "SupportedFileFormat":
        with open(file, "rb") as fp:
            yield cls.from_file_pointer_and_format(fp, file.suffix)

    @classmethod
    def from_file_pointer_and_format(
        cls, fp: IOBase, file_format: str
    ) -> "SupportedFileFormat":
        # Import all file formats so that they can register themselves
        cls.import_all()
        file_format = SUPPORTED_FILE_FORMAT_REGISTRY.get(file_format)
        return file_format(fp)

    @abstractmethod
    def read_file_from_handle(self, fp: BufferedReader) -> Iterable[JsonLikeDocument]:
        ...


@SUPPORTED_COMPRESSED_FILE_FORMAT_REGISTRY.connect_baseclass
class SupportedCompressedFileFormat(Pluggable, ABC):
    def __init__(self, file: Union[Path, IOBase]) -> None:
        self.file = file

    @classmethod
    def from_file_pointer_and_format(
        cls, fp: IOBase, format: str
    ) -> "SupportedCompressedFileFormat":
        # Import all compression file formats so that they can register themselves
        cls.import_all()
        file_format = SUPPORTED_COMPRESSED_FILE_FORMAT_REGISTRY.get(format)
        return file_format(fp)

    @abstractmethod
    def decompress_file(self) -> Path:
        ...

    @abstractmethod
    def decompress_bytes(self) -> BytesIO:
        ...


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
    def decompress_file(self) -> Path:
        output_path = self.file.with_suffix("")
        with gzip.open(self.file, "rb") as f_in, open(output_path, "wb") as f_out:
            chunk_size = 65536
            while True:
                chunk = f_in.read(chunk_size)
                if not chunk:
                    break
                f_out.write(chunk)
        return output_path

    def decompress_bytes(self) -> BytesIO:
        decompressed_stream = BytesIO()
        with gzip.GzipFile(fileobj=self.file, mode="rb") as gz:
            while True:
                chunk = gz.read(1024)
                if not chunk:
                    break
                decompressed_stream.write(chunk)
        decompressed_stream.seek(0)
        return decompressed_stream


class Bz2FileFormat(SupportedCompressedFileFormat, alias=".bz2"):
    def decompress_file(self) -> Path:
        output_path = self.file.with_suffix("")
        with bz2.open(self.file, "rb") as f_in, open(output_path, "wb") as f_out:
            chunk_size = 65536
            while True:
                chunk = f_in.read(chunk_size)
                if not chunk:
                    break
                f_out.write(chunk)
        return output_path

    def decompress_bytes(self) -> BytesIO:
        with bz2.open(self.file, "rb") as compressed_file:
            decompressed_data = compressed_file.read()
        decompressed_stream = BytesIO(decompressed_data)
        decompressed_stream.seek(0)
        return decompressed_stream


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
            with SupportedFileFormat.open(resolve_compressed_path(path)) as file:
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


# decompress all in path, return uncompressed path
def resolve_compressed_path(path: Path) -> Path:
    while path.suffix in SUPPORTED_COMPRESSED_FILE_FORMAT_REGISTRY:
        compress_file_format = (
            SupportedCompressedFileFormat.from_file_pointer_and_format(
                path, path.suffix
            )
        )
        path = compress_file_format.decompress_file()
    return path
