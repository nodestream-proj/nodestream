import bz2
import gzip
import json
import sys
import tempfile
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from csv import DictReader
from glob import glob
from io import BufferedReader, BytesIO, IOBase, TextIOWrapper
from logging import getLogger
from pathlib import Path
from typing import (
    Any,
    AsyncContextManager,
    AsyncGenerator,
    AsyncIterator,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
)

import pandas as pd
from httpx import AsyncClient
from yaml import safe_load

from ...model import JsonLikeDocument
from ...pluggable import Pluggable
from ...subclass_registry import MissingFromRegistryError, SubclassRegistry
from .credential_utils import AwsClientFactory
from .extractor import Extractor

SUPPORTED_FILE_FORMAT_REGISTRY = SubclassRegistry()
SUPPORTED_COMPRESSED_FILE_FORMAT_REGISTRY = SubclassRegistry()
SUPPORTED_FILE_SOURCES_REGISTRY = SubclassRegistry()


class Utf8TextIOWrapper(TextIOWrapper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, encoding="utf-8")


class ReadableFile(ABC):
    @abstractmethod
    def as_reader(self, cls: type[IOBase]) -> AsyncContextManager[IOBase]:
        """Return a reader for the file.

        Given a class that represents a file reader, return an async context
        manager that yields an instance of that class. This allows different
        file types to be read in a uniform way.

        The context manager will close when the file is done being read and
        is no longer needed by the pipeline. Therefore, you can perform cleanup
        operations in the context manager on exit
        (i.e after the yield statement).
        """

    @abstractmethod
    def path_like(self) -> Path:
        """Return a Path object that represents the path to the file.

        This method should return a Path object that represents the path to the
        file. Note, the path does not need to be an actual file on the
        filesystem. It can be a URL or any other path-like object that can
        be used to identify the file.

        This will be called to sniff the file format and compression format of
        the file from the suffixes of the path.
        """

    @asynccontextmanager
    async def popped_suffix_tempfile(
        self,
    ) -> AsyncContextManager[tuple[Path, tempfile.NamedTemporaryFile]]:
        """Create a temporary file with the same suffixes sans the last one.

        This method creates a temporary file with the same suffixes as the
        original file sans the last one. This is useful when creating
        intermediary files that need to be passed through multiple codecs.

        For instance, imagine a file such as foo.txt.gz. When the file is
        being ingested, we will pass the file through the decompression codec
        and then through the file format codec. The decompression codec will
        remove the .gz suffix. And store it in an intermediary file which can
        be read by the file format codec (.txt in this case).
        """
        new = self.path_like().with_suffix("")
        with tempfile.NamedTemporaryFile(suffix="".join(new.suffixes)) as fp:
            yield Path(fp.name), fp

    def __repr__(self) -> str:
        return str(self.path_like())


class LocalFile(ReadableFile):
    """A class that represents a local file that can be read.

    This class is used to read files from the local filesystem. The class
    takes a Path object that represents the path to the file. The class
    uses the Path object to open the file and return a reader that can be
    used to read the file.
    """

    def __init__(self, path: Path, pointer=None) -> None:
        self.path = path
        self.pointer = pointer

    @asynccontextmanager
    async def as_reader(self, cls: type[IOBase]):
        if self.pointer:
            yield cls(self.pointer)
        else:
            with self.path.open("rb") as fp:
                yield cls(fp)

    def path_like(self) -> Path:
        return self.path


class RemoteFile(ReadableFile):
    """A class that represents a remote file that can be read.

    This class is used to read files from a remote URL. The class uses the
    httpx library to stream the file from the URL and return a reader that
    can be used to read the file.
    """

    def __init__(
        self, url: str, client: AsyncClient, memory_spooling_in_mb: int
    ) -> None:
        self.url = url
        self.client = client
        self.max_memory_spooling = memory_spooling_in_mb * 1024 * 1024

    @asynccontextmanager
    async def as_reader(self, cls: type[IOBase]):
        if sys.version_info >= (3, 11):
            temp_file = tempfile.SpooledTemporaryFile(max_size=self.max_memory_spooling)
        else:
            temp_file = tempfile.TemporaryFile()

        with temp_file as fp:
            async with self.client.stream("GET", self.url) as response:
                async for chunk in response.aiter_bytes():
                    fp.write(chunk)
            fp.seek(0)
            yield cls(fp)

    def path_like(self) -> Path:
        return Path(self.url)


@SUPPORTED_FILE_SOURCES_REGISTRY.connect_baseclass
class FileSource(Pluggable, ABC):
    entrypoint_name = "file_sources"

    @classmethod
    def from_file_data_with_type_label(cls, data: Dict[str, Any]) -> "FileSource":
        type = data.pop("type")
        return SUPPORTED_FILE_SOURCES_REGISTRY.get(type).from_file_data(**data)

    @classmethod
    def from_file_data(cls, **kwargs) -> "FileSource":
        return cls(**kwargs)

    @abstractmethod
    def get_files(self) -> AsyncIterator[ReadableFile]:
        """Return an async iterator of files to be processed.

        This method should return an async iterator that yields instances of
        ReadableFile. The ReadableFile instances should be able to be read
        by the Extractor class. This method is used to abstract away the
        details of how files are read from different sources (i.e local files,
        remote files, etc).

        The async iterator should yield all the files that need to be processed
        by the pipeline. The Extractor class will then read the files and
        extract the records from them.
        """
        raise NotImplementedError

    @abstractmethod
    def describe(self) -> str:
        """Return a human-readable description of the file source.

        This method should return a human-readable description of the file source.
        The description should be a string that describes the file source in a
        way that is understandable to the user. The description should be
        concise and informative.
        """


@SUPPORTED_FILE_FORMAT_REGISTRY.connect_baseclass
class FileCodec(Pluggable, ABC):
    """Base class for file codecs.

    This class is used to define the interface for file codecs. File codecs
    are used to read files of a specific format. The class should implement
    the read_file_from_handle method which reads the file and returns an
    iterable of records.

    A class can define the `reader` attribute to specify the class that should
    be used to read the file. If the `reader` attribute is not defined, the
    default class used to read the file is a simple base io reader. In other
    words, if you don't define the `reader` attribute, the file will be read
    as a binary file. If you want to read the file as a text file, you should
    define the `reader` attribute as TextIOWrapper.
    """

    reader = None
    entrypoint_name = "file_formats"

    @abstractmethod
    def read_file_from_handle(self, reader: IOBase) -> Iterable[JsonLikeDocument]:
        """Read the file from the reader and return an iterable of records.

        This method should read the file from the reader and return an iterable
        of records. The records should be in a format that can be processed by
        the pipeline and be "JSON-like" (i.e a dictionary, list, etc).
        The method should yield the records one by one as they are read from
        the file.
        """
        raise NotImplementedError


@SUPPORTED_COMPRESSED_FILE_FORMAT_REGISTRY.connect_baseclass
class CompressionCodec(Pluggable, ABC):
    """Base class for compression codecs.

    This class is used to define the interface for compression codecs.
    Compression codecs are used to decompress files that are compressed
    using a specific compression algorithm. The class should implement the
    decompress_file method which decompresses the file and returns a new file
    that can be read by the file format codec or another decompression codec.
    """

    def __init__(self, file: ReadableFile) -> None:
        self.file = file

    @abstractmethod
    def decompress_file(self) -> AsyncContextManager[ReadableFile]:
        """Decompress the file and return a new file that can be read.

        This method should decompress the file and return a new file that can
        be read by the file format codec or another decompression codec.
        The method should return a new instance of ReadableFile that can be
        read by the pipeline.

        `ReadableFile` contains a helper method `popped_suffix_tempfile` that
        can be used to create a new file with the same suffixes as the original
        file sans the last suffix. This is useful when creating intermediary
        files that need to be passed through multiple codecs.

        For instance, imagine a file such as foo.txt.gz. When the file is
        being ingested, we will pass the file through the decompression codec
        and then through the file format codec. The decompression codec will
        remove the .gz suffix. And store it in an intermediary file which can
        be read by the file format codec (.txt in this case).
        """
        raise NotImplementedError


class JsonFileFormat(FileCodec, alias=".json"):
    """File format codec for JSON files.

    This class is used to read JSON files. The class reads the file and
    returns an iterable of records. The records are read one by one from
    the file and yielded as they are read.

    The class is registered with the file format registry using the alias
    ".json". This means that when the pipeline encounters a file with the
    suffix .json, it will use this class to read the file.
    """

    reader = Utf8TextIOWrapper

    def read_file_from_handle(
        self, reader: TextIOWrapper
    ) -> Iterable[JsonLikeDocument]:
        return [json.load(reader)]


class LineSeperatedJsonFileFormat(FileCodec, alias=".jsonl"):
    """File format codec for line separated JSON files.

    This class is used to read line separated JSON files. The class reads the
    file and returns an iterable of records. The records are read one by one
    from the file and yielded as they are read.

    The class is registered with the file format registry using the alias
    ".jsonl". This means that when the pipeline encounters a file with the
    suffix .jsonl, it will use this class to read the file.

    The class reads the file line by line and loads each line as a JSON object.
    """

    reader = Utf8TextIOWrapper

    def read_file_from_handle(
        self, reader: TextIOWrapper
    ) -> Iterable[JsonLikeDocument]:
        return (json.loads(line.strip()) for line in reader.readlines())


class ParquetFileFormat(FileCodec, alias=".parquet"):
    """File format codec for Parquet files.

    This class is used to read Parquet files. The class reads the file and
    returns an iterable of records. The records are read one by one from
    the file and yielded as they are read.

    The class is registered with the file format registry using the alias
    ".parquet". This means that when the pipeline encounters a file with the
    suffix .parquet, it will use this class to read the file.

    The class reads the file using the pandas library and yields the records
    one by one as they are read.
    """

    def read_file_from_handle(self, fp: IOBase) -> Iterable[JsonLikeDocument]:
        df = pd.read_parquet(fp, engine="pyarrow")
        return (row[1].to_dict() for row in df.iterrows())


class TextFileFormat(FileCodec, alias=".txt"):
    """File format codec for text files.

    This class is used to read text files. The class reads the file and
    returns an iterable of records. The records are read one by one from
    the file and yielded as they are read.

    The class is registered with the file format registry using the alias
    ".txt". This means that when the pipeline encounters a file with the
    suffix .txt, it will use this class to read the file.

    The class reads the file line by line and yields each line as a record in
    a dictionary with a single key "line".
    """

    reader = Utf8TextIOWrapper

    def read_file_from_handle(
        self, reader: TextIOWrapper
    ) -> Iterable[JsonLikeDocument]:
        return ({"line": line.strip()} for line in reader.readlines())


class CommaSeperatedValuesFileFormat(FileCodec, alias=".csv"):
    """File format codec for CSV files.

    This class is used to read CSV files. The class reads the file and
    returns an iterable of records. The records are read one by one from
    the file and yielded as they are read.

    The class is registered with the file format registry using the alias
    ".csv". This means that when the pipeline encounters a file with the
    suffix .csv, it will use this class to read the file.

    The class reads the file using the csv.DictReader class and yields the
    records one by one as they are read
    """

    reader = Utf8TextIOWrapper

    def read_file_from_handle(
        self, reader: TextIOWrapper
    ) -> Iterable[JsonLikeDocument]:
        return DictReader(reader)


class YamlFileFormat(FileCodec, alias=".yaml"):
    """File format codec for YAML files.

    This class is used to read YAML files. The class reads the file and
    returns an iterable of records. The records are read one by one from
    the file and yielded as they are read.

    The class is registered with the file format registry using the alias
    ".yaml". This means that when the pipeline encounters a file with the
    suffix .yaml, it will use this class to read the file.

    The class reads the file using the yaml.safe_load function and yields one
    record which is the entire file.
    """

    reader = Utf8TextIOWrapper

    def read_file_from_handle(
        self, reader: TextIOWrapper
    ) -> Iterable[JsonLikeDocument]:
        return [safe_load(reader)]


class TempFile(LocalFile):
    def __init__(self, path: Path, pointer=None, original_path: str = ""):
        super().__init__(path, pointer)
        self.original_path = original_path or self.path.name

    def __repr__(self):
        return self.original_path if self.original_path else super().__repr__()


class GzipFileFormat(CompressionCodec, alias=".gz"):
    """Compression codec for Gzip files.

    This class is used to decompress Gzip files. The class takes a file that
    is compressed using the Gzip compression algorithm and decompresses it.
    The class returns a new file that can be read by the file format codec
    or another decompression codec.

    The class is registered with the compression codec registry using the
    alias ".gz". This means that when the pipeline encounters a file with
    the suffix .gz, it will use this class to decompress the file.
    """

    @asynccontextmanager
    async def decompress_file(self) -> AsyncIterator[ReadableFile]:
        async with self.file.popped_suffix_tempfile() as (new_path, temp_file):
            async with self.file.as_reader(BufferedReader) as reader:
                with gzip.GzipFile(fileobj=reader) as decompressor:
                    temp_file.write(decompressor.read())
            temp_file.seek(0)
            yield TempFile(new_path, temp_file, str(self.file))


class Bz2FileFormat(CompressionCodec, alias=".bz2"):
    """Compression codec for BZ2 files.

    This class is used to decompress BZ2 files. The class takes a file that
    is compressed using the BZ2 compression algorithm and decompresses it.
    The class returns a new file that can be read by the file format codec
    or another decompression codec.

    The class is registered with the compression codec registry using the
    alias ".bz2". This means that when the pipeline encounters a file with
    the suffix .bz2, it will use this class to decompress the file.
    """

    @asynccontextmanager
    async def decompress_file(self) -> AsyncIterator[ReadableFile]:
        async with self.file.popped_suffix_tempfile() as (new_path, temp_file):
            async with self.file.as_reader(BufferedReader) as reader:
                decompressor = bz2.BZ2Decompressor()
                for chunk in iter(lambda: reader.read(1024 * 1024), b""):
                    temp_file.write(decompressor.decompress(chunk))
            temp_file.seek(0)
            yield TempFile(new_path, temp_file, str(self.file))


class LocalFileSource(FileSource, alias="local"):
    """A class that represents a source of local files to be read.

    This class is used to read files from the local filesystem. The class
    takes a list of glob patterns that are used to find files on the local
    filesystem. The class uses the glob module to find all the files that
    match the glob patterns and then yields instances of LocalFile that
    can be read by the pipeline.
    """

    @classmethod
    def from_file_data(cls, globs: List[str]) -> "LocalFileSource":
        all_matches = (
            Path(file)
            for glob_string in globs
            for file in glob(glob_string, recursive=True)
        )
        return cls(list(sorted(filter(Path.is_file, all_matches))))

    def __init__(self, paths: List[Path]) -> None:
        self.paths = paths

    async def get_files(self) -> AsyncIterator[ReadableFile]:
        for path in self.paths:
            yield LocalFile(path)

    def describe(self) -> str:
        if len(self.paths) == 1:
            return f"{self.paths[0]}"
        else:
            return f"{len(self.paths)} local files"


class RemoteFileSource(FileSource, alias="http"):
    """A class that represents a source of remote files to be read.

    This class is used to read files from remote URLs. The class takes a list
    of URLs and a memory spooling maximum size in MB. The class uses the httpx
    library to stream the files from the URLs and yield instances of RemoteFile
    that can be read by the pipeline.
    """

    def __init__(
        self, urls: Sequence[str], memory_spooling_max_size_in_mb: int = 10
    ) -> None:
        self.urls = urls
        self.memory_spooling_max_size = memory_spooling_max_size_in_mb * 1024 * 1024

    async def get_files(self) -> AsyncIterator[ReadableFile]:
        async with AsyncClient() as client:
            for url in self.urls:
                yield RemoteFile(url, client, self.memory_spooling_max_size)

    def describe(self) -> str:
        if len(self.urls) == 1:
            return f"{self.urls[0]}"
        else:
            return f"{len(self.urls)} remote files"


class S3File(ReadableFile):
    """A readable file that is stored in S3.

    This class is used to read files from S3. The class takes a key, bucket,
    and an S3 client. The class uses the S3 client to get the object from S3
    and yield an instance of the file that can be read by the pipeline.

    The class also has a method to archive the file after it has been read.

    """

    def __init__(
        self,
        key: str,
        s3_client,
        bucket: str,
        archive_dir: str | None,
        object_format: str | None,
    ) -> None:
        self.logger = getLogger(__name__)
        self.key = key
        self.s3_client = s3_client
        self.bucket = bucket
        self.archive_dir = archive_dir
        self.object_format = object_format

    def archive_if_required(self, key: str):
        if not self.archive_dir:
            return

        self.logger.info("Archiving S3 Object", extra={"key": key})
        filename = Path(key).name
        self.s3_client.copy(
            Bucket=self.bucket,
            Key=f"{self.archive_dir}/{filename}",
            CopySource={"Bucket": self.bucket, "Key": key},
        )
        self.s3_client.delete_object(Bucket=self.bucket, Key=key)

    def path_like(self) -> Path:
        path = Path(self.key)
        if self.object_format:
            return path.with_suffix(self.object_format)

        return path

    @asynccontextmanager
    async def as_reader(self, reader: IOBase):
        streaming_body = self.s3_client.get_object(Bucket=self.bucket, Key=self.key)[
            "Body"
        ]
        yield reader(BytesIO(streaming_body.read()))
        self.archive_if_required(self.key)

    def __repr__(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


class S3FileSource(FileSource, alias="s3"):
    """A class that represents a source of files stored in S3.

    This class is used to read files from S3. The class takes a bucket, prefix,
    and an S3 client. The class uses the S3 client to list the objects in the
    bucket and yield instances of S3File that can be read by the pipeline.

    The class also has a method to archive the file after it has been read.

    The class can also filter the objects returned by the prefix scan in the
    following ways:
    - Specifying object_format OR suffix will filter the objects via endswith
    - Providing strings to object_format AND suffix will:
        - Filter the objects via endswith with suffix (a blank string will match all)
        - Process each object as if it ended with the contents of object_format
    """

    @classmethod
    def from_file_data(
        cls,
        bucket: str,
        prefix: Optional[str] = None,
        suffix: Optional[str] = None,
        archive_dir: Optional[str] = None,
        object_format: Optional[str] = None,
        **aws_client_args,
    ):
        return cls(
            bucket=bucket,
            prefix=prefix,
            suffix=suffix,
            archive_dir=archive_dir,
            object_format=object_format,
            s3_client=AwsClientFactory(**aws_client_args).make_client("s3"),
        )

    def __init__(
        self,
        *,
        bucket: str,
        s3_client,
        archive_dir: Optional[str] = None,
        object_format: Optional[str] = None,
        prefix: Optional[str] = None,
        suffix: Optional[str] = None,
    ):
        self.bucket = bucket
        self.s3_client = s3_client
        self.archive_dir = archive_dir
        self.object_format = object_format
        self.prefix = prefix or ""
        self.suffix = suffix

    def object_is_not_in_archive(self, key: str) -> bool:
        return not key.startswith(self.archive_dir) if self.archive_dir else True

    def key_matches_suffix(self, key: str) -> bool:
        if self.suffix is not None:
            return key.endswith(self.suffix)

        if self.object_format:
            return key.endswith(self.object_format)

        return True

    def find_keys_in_bucket(self) -> Iterable[str]:
        # Returns all keys in the bucket that are not in the archive dir,
        # have the object_format suffix and have the prefix.
        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)
        for page in page_iterator:
            keys = (obj["Key"] for obj in page.get("Contents", []))
            yield from (
                filter(
                    self.key_matches_suffix,
                    filter(
                        self.object_is_not_in_archive,
                        keys,
                    ),
                )
            )

    async def get_files(self):
        for key in self.find_keys_in_bucket():
            yield S3File(
                key=key,
                s3_client=self.s3_client,
                bucket=self.bucket,
                archive_dir=self.archive_dir,
                # for backwards compatibility:
                # -- Only override object_format if suffix is provided.
                # -- To treat ALL files as object_format, set suffix to "".
                object_format=(
                    self.object_format
                    if self.object_format and self.suffix is not None
                    else None
                ),
            )

    def describe(self) -> str:
        data = {
            k: v
            for k, v in {
                "bucket": self.bucket,
                "archive_dir": self.archive_dir,
                "object_format": self.object_format,
                "prefix": self.prefix,
                "suffix": self.suffix,
            }.items()
            if v
        }
        return f"S3FileSource{data}"

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, S3FileSource)
            and self.s3_client == other.s3_client
            and self.bucket == other.bucket
            and self.prefix == other.prefix
            and self.archive_dir == other.archive_dir
            and self.object_format == other.object_format
        )


class FileExtractor(Extractor):
    """A class that extracts records from files.

    This class is used to extract records from files. The class takes a list
    of file sources that are used to read the files. The class uses the file
    sources to get a list of files and then reads the files using the file
    codecs. The class yields the records one by one as they are read from the
    files.
    """

    @classmethod
    def local(cls, globs: Iterable[str]) -> "FileExtractor":
        return FileExtractor.from_file_data([{"type": "local", "globs": globs}])

    @classmethod
    def s3(cls, **kwargs) -> "FileExtractor":
        return cls([S3FileSource.from_file_data(**kwargs)])

    @classmethod
    def remote(
        cls,
        urls: Iterable[str],
        memory_spooling_max_size_in_mb: int = 10,
    ) -> "FileExtractor":
        return FileExtractor.from_file_data(
            [
                {
                    "type": "http",
                    "urls": urls,
                    "memory_spooling_max_size_in_mb": memory_spooling_max_size_in_mb,
                }
            ]
        )

    @classmethod
    def from_file_data(cls, sources: list[dict[str, Any]]) -> "FileExtractor":
        return cls(
            [FileSource.from_file_data_with_type_label(source) for source in sources]
        )

    def __init__(self, file_sources: Sequence[FileSource]) -> None:
        self.file_sources = file_sources
        self.logger = getLogger(__name__)

    async def read_file(
        self,
        file: ReadableFile,
    ) -> AsyncGenerator[JsonLikeDocument, None]:
        intermediaries: list[AsyncContextManager[ReadableFile]] = []

        while True:
            suffix = file.path_like().suffix

            # Try to find a compression codec that can decompress the file.
            # If a compression codec is found, decompress the file and store
            # the decompressed file in an intermediary file. The intermediary
            # file will be read by the file format codec.
            try:
                algo = SUPPORTED_COMPRESSED_FILE_FORMAT_REGISTRY.get(suffix)(file)
                decompression = algo.decompress_file()
                file = await decompression.__aenter__()
                intermediaries.append(decompression)
                continue
            except MissingFromRegistryError:
                pass
            except OSError as e:
                self.logger.warning(
                    "Failed to decompress %s file. "
                    "Please ensure the file is in the correct format.",
                    file,
                    extra={"exception": str(e)},
                )
                break

            # If we didn't find a compression codec, try to find a file format
            # codec that can read the file. If a file format codec is found,
            # read the file and yield the records.
            try:
                codec = SUPPORTED_FILE_FORMAT_REGISTRY.get(suffix)()
                async with file.as_reader(codec.reader or BufferedReader) as reader:
                    for record in codec.read_file_from_handle(reader):
                        yield record
            except MissingFromRegistryError:
                # If we didn't find a file format codec, break out of the loop
                # and yield no records.
                pass
            except Exception as e:
                self.logger.warning(
                    "Failed to parse %s file (at path %s). "
                    "Please ensure the file is in the correct format.",
                    file,
                    file.path_like(),
                    extra={"exception": str(e)},
                )

            # Regardless of whether we found a codec or not, break out of the
            # loop and yield no more records because either (a) we found a
            # codec and read the file or (b) we didn't find a codec and
            # couldn't read the file. Trying again would be futile.
            break

        # Cleanup the intermediary files.
        for intermediary in reversed(intermediaries):
            await intermediary.__aexit__(None, None, None)

    async def extract_records(self) -> AsyncGenerator[Any, Any]:
        for file_source in self.file_sources:
            total_files_from_source = 0
            async for file in file_source.get_files():
                total_files_from_source += 1
                async for record in self.read_file(file):
                    yield record

            if total_files_from_source == 0:
                self.logger.warning(
                    "No files found for source: %s",
                    file_source.describe(),
                )
