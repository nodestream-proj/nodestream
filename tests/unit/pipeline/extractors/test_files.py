import bz2
import csv
import gzip
import json
import typing
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import AsyncIterator

import pandas as pd
import pytest
import yaml
from hamcrest import assert_that, equal_to

from nodestream.pipeline.extractors.files import (
    FileExtractor,
    FileSource,
    LocalFileSource,
    ReadableFile,
    RemoteFileSource,
)

SIMPLE_RECORD = {"record": "value"}


@pytest.fixture(autouse=True)
def base_dir():
    with TemporaryDirectory() as tempdir:
        yield tempdir


@pytest.fixture(autouse=True)
def unsupported_file(base_dir: str):
    with NamedTemporaryFile("w+", suffix=".unsupported", dir=base_dir) as temp_file:
        temp_file.write("hello world")
        temp_file.seek(0)
        yield temp_file


@pytest.fixture(autouse=True)
def json_file(base_dir: str):
    with NamedTemporaryFile("w+", suffix=".json", dir=base_dir) as temp_file:
        json.dump(SIMPLE_RECORD, temp_file)
        temp_file.seek(0)
        yield temp_file


@pytest.fixture
def empty_json_file(base_dir: str):
    with NamedTemporaryFile("w+", suffix=".json", dir=base_dir) as temp_file:
        yield temp_file


@pytest.fixture(autouse=True)
def jsonl_file(base_dir: str):
    with NamedTemporaryFile("w+", suffix=".jsonl", dir=base_dir) as temp_file:
        json.dump(SIMPLE_RECORD, temp_file)
        temp_file.write("\n")
        json.dump(SIMPLE_RECORD, temp_file)
        temp_file.seek(0)
        yield temp_file


@pytest.fixture(autouse=True)
def csv_file(base_dir: str):
    with NamedTemporaryFile("w+", suffix=".csv", dir=base_dir) as temp_file:
        writer = csv.DictWriter(temp_file, SIMPLE_RECORD.keys())
        writer.writeheader()
        writer.writerow(SIMPLE_RECORD)
        temp_file.seek(0)
        yield temp_file


@pytest.fixture(autouse=True)
def txt_file(base_dir: str):
    with NamedTemporaryFile("w+", suffix=".txt", dir=base_dir) as temp_file:
        temp_file.write("hello world")
        temp_file.seek(0)
        yield temp_file


@pytest.fixture(autouse=True)
def yaml_file(base_dir: str):
    with NamedTemporaryFile("w+", suffix=".yaml", dir=base_dir) as temp_file:
        yaml.dump(SIMPLE_RECORD, temp_file)
        temp_file.seek(0)
        yield temp_file


@pytest.fixture(autouse=True)
def parquet_file(base_dir: str):
    with NamedTemporaryFile("w+", suffix=".parquet", dir=base_dir) as temp_file:
        df = pd.DataFrame(data=SIMPLE_RECORD, index=[0])
        df.to_parquet(temp_file.name)
        temp_file.seek(0)
        yield temp_file


@pytest.fixture(autouse=True)
def gzip_file(base_dir: str):
    with NamedTemporaryFile("wb", suffix=".json.gz", dir=base_dir) as temp_file:
        json_data = json.dumps(SIMPLE_RECORD).encode("utf-8")
        with gzip.GzipFile(temp_file.name, mode="wb") as gzip_file:
            gzip_file.write(json_data)
        if not temp_file.name.endswith(".json.gz"):
            raise ValueError("not a json gzip file")
        temp_file.seek(0)
        yield temp_file


@pytest.fixture(autouse=True)
def bz2_file(base_dir: str):
    with NamedTemporaryFile("wb", suffix=".json.bz2", dir=base_dir) as temp_file:
        json_data = json.dumps(SIMPLE_RECORD).encode("utf-8")
        with bz2.BZ2File(temp_file.name, mode="wb") as bz2_file:
            bz2_file.write(json_data)
        if not temp_file.name.endswith(".json.bz2"):
            raise ValueError("not a json bz2 file")
        temp_file.seek(0)
        yield temp_file


@pytest.fixture(scope="function")
def file_path(
    request,
    unsupported_file,
    json_file,
    empty_json_file,
    csv_file,
    txt_file,
    yaml_file,
    parquet_file,
    gzip_file,
    bz2_file,
    jsonl_file,
):
    # This is a hack to allow us to use the auto-cleanup of the file fixtures
    # but also do a parametrized test
    match request.param:
        case "unsupported":
            return Path(unsupported_file.name)
        case "json":
            return Path(json_file.name)
        case "empty_json":
            return Path(empty_json_file.name)
        case "csv":
            return Path(csv_file.name)
        case "txt":
            return Path(txt_file.name)
        case "yaml":
            return Path(yaml_file.name)
        case "parquet":
            return Path(parquet_file.name)
        case "gzip":
            return Path(gzip_file.name)
        case "bz2":
            return Path(bz2_file.name)
        case "jsonl":
            return Path(jsonl_file.name)
        case _:
            raise ValueError("unknown path type")


@pytest.mark.parametrize(
    "file_path,expected",
    [
        ("unsupported", []),
        ("json", [SIMPLE_RECORD]),
        ("empty_json", []),
        ("csv", [SIMPLE_RECORD]),
        ("txt", [{"line": "hello world"}]),
        ("yaml", [SIMPLE_RECORD]),
        ("parquet", [SIMPLE_RECORD]),
        ("gzip", [SIMPLE_RECORD]),
        ("bz2", [SIMPLE_RECORD]),
        ("jsonl", [SIMPLE_RECORD, SIMPLE_RECORD]),
    ],
    indirect=["file_path"],
)
@pytest.mark.asyncio
async def test_file(file_path, expected):
    subject = FileExtractor([LocalFileSource([file_path])])
    assert [r async for r in subject.extract_records()] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "glob, expected",
    [
        ("*.json", [SIMPLE_RECORD]),
        ("*.csv", [SIMPLE_RECORD]),
        ("*.gz", [SIMPLE_RECORD]),
        ("*.bz2", [SIMPLE_RECORD]),
        ("*.yaml", [SIMPLE_RECORD]),
        ("*.parquet", [SIMPLE_RECORD]),
        ("*.unsupported", []),
        ("*.txt", [{"line": "hello world"}]),
        ("*.jsonl", [SIMPLE_RECORD, SIMPLE_RECORD]),
    ],
)
async def test_formatting_unified(glob: str, expected, base_dir: str):
    subject = FileExtractor.from_file_data(
        [{"type": "local", "globs": [f"{base_dir}/{glob}"]}]
    )
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to(expected))


def test_declarative_init(
    base_dir,
    csv_file,
    json_file,
    jsonl_file,
    txt_file,
    yaml_file,
    bz2_file,
    gzip_file,
    unsupported_file,
    parquet_file,
):
    subject = FileExtractor.local(globs=[f"{base_dir}/**"])
    assert isinstance(subject.file_sources[0], LocalFileSource)
    local_source: LocalFileSource = typing.cast(
        LocalFileSource,
        subject.file_sources[0],
    )
    assert set(local_source.paths) == {
        Path(f.name)
        for f in [
            csv_file,
            json_file,
            txt_file,
            jsonl_file,
            yaml_file,
            bz2_file,
            gzip_file,
            unsupported_file,
            parquet_file,
        ]
    }


@pytest.mark.asyncio
async def test_remote_file_extractor_extract_records(httpx_mock):
    files = ["https://example.com/file.json", "https://example.com/file2.json"]
    for file in files:
        httpx_mock.add_response(
            url=file,
            method="GET",
            json=SIMPLE_RECORD,
        )
    subject = FileExtractor.remote(urls=files)
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD, SIMPLE_RECORD]))


@pytest.mark.asyncio
async def test_no_files_found_from_local_source(mocker):
    file_source = LocalFileSource([])
    subject = FileExtractor([file_source])
    subject.logger = mocker.Mock()
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([]))
    subject.logger.warning.assert_called_once_with(
        "No files found for source: %s",
        "0 local files",
    )


@pytest.mark.asyncio
async def test_no_files_found_from_remote_source(mocker):
    subject = FileExtractor([RemoteFileSource([], 10)])
    subject.logger = mocker.Mock()
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([]))
    subject.logger.warning.assert_called_once_with(
        "No files found for source: %s",
        "0 remote files",
    )


def test_remote_file_source_single_file_description():
    url = "https://example.com/file.json"
    subject = RemoteFileSource([url], 10)
    assert_that(subject.describe(), equal_to(url))


def test_remote_file_source_multiple_file_description():
    urls = ["https://example.com/file.json", "https://example.com/file2.json"]
    subject = RemoteFileSource(urls, 10)
    assert_that(subject.describe(), equal_to("2 remote files"))


def test_local_file_source_single_file_description(base_dir: str):
    path = Path(f"{base_dir}/file.json")
    subject = LocalFileSource([path])
    assert_that(subject.describe(), equal_to(str(path)))


def test_local_file_source_multiple_file_description(base_dir: str):
    paths = [
        Path(f"{base_dir}/file.json"),
        Path(f"{base_dir}/file2.json"),
    ]
    subject = LocalFileSource(paths)
    assert_that(subject.describe(), equal_to("2 local files"))


def test_file_source_default_description():
    class SomeFileSource(FileSource):
        def get_files(self) -> AsyncIterator[ReadableFile]:
            pass  # not implemented for this test

        def describe(self) -> str:
            return "SomeFileSource(test)"

    subject = SomeFileSource()
    assert_that(subject.describe(), equal_to("SomeFileSource(test)"))


@pytest.mark.asyncio
async def test_unified_extractor_multiple_source_types(json_file, csv_file, httpx_mock):
    urls = ["https://example.com/file.json", "https://example.com/file2.json"]
    for url in urls:
        httpx_mock.add_response(
            url=url,
            method="GET",
            json=SIMPLE_RECORD,
        )

    subject = FileExtractor.from_file_data(
        [
            {"type": "local", "globs": [json_file.name]},
            {"type": "local", "globs": [csv_file.name]},
            {"type": "http", "urls": urls},
        ]
    )
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD] * 4))
