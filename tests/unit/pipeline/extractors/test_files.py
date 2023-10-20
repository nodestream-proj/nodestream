import csv
import itertools
import json
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pytest
from hamcrest import assert_that, equal_to, has_length

from nodestream.pipeline.extractors.files import FileExtractor, RemoteFileExtractor

SIMPLE_RECORD = {"record": "value"}


@pytest.fixture
def fixture_directory():
    with TemporaryDirectory() as tempdir:
        yield tempdir


@pytest.fixture
def json_file(fixture_directory):
    with NamedTemporaryFile(
        "w+", suffix=".json", dir=fixture_directory, delete=False
    ) as temp_file:
        name = temp_file.name
        json.dump(SIMPLE_RECORD, temp_file)
        temp_file.seek(0)
    yield Path(name)


@pytest.fixture
def jsonl_file(fixture_directory):
    with NamedTemporaryFile(
        "w+", suffix=".jsonl", dir=fixture_directory, delete=False
    ) as temp_file:
        name = temp_file.name
        json.dump(SIMPLE_RECORD, temp_file)
        temp_file.write("\n")
        json.dump(SIMPLE_RECORD, temp_file)
        temp_file.seek(0)
    yield Path(name)


@pytest.fixture
def csv_file(fixture_directory):
    with NamedTemporaryFile(
        "w+", suffix=".csv", dir=fixture_directory, delete=False
    ) as temp_file:
        name = temp_file.name
        writer = csv.DictWriter(temp_file, SIMPLE_RECORD.keys())
        writer.writeheader()
        writer.writerow(SIMPLE_RECORD)
        temp_file.seek(0)
    yield Path(name)


@pytest.fixture
def txt_file(fixture_directory):
    with NamedTemporaryFile(
        "w+", suffix=".txt", dir=fixture_directory, delete=False
    ) as temp_file:
        name = temp_file.name
        temp_file.write("hello world")
        temp_file.seek(0)
    yield Path(name)


@pytest.mark.asyncio
async def test_json_formatting(json_file):
    subject = FileExtractor([json_file])
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD]))


@pytest.mark.asyncio
async def test_csv_formatting(csv_file):
    subject = FileExtractor([csv_file])
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD]))


@pytest.mark.asyncio
async def test_txt_formatting(txt_file):
    subject = FileExtractor([txt_file])
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([{"line": "hello world"}]))


@pytest.mark.asyncio
async def test_jsonl_formatting(jsonl_file):
    subject = FileExtractor([jsonl_file])
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD, SIMPLE_RECORD]))


def test_declarative_init(fixture_directory, csv_file, json_file, txt_file, jsonl_file):
    subject = FileExtractor.from_file_data(globs=[f"{fixture_directory}/**"])
    assert_that(list(subject.paths), has_length(4))


@pytest.mark.asyncio
async def test_remote_file_extractor_extract_records(mocker, httpx_mock):
    files = ["https://example.com/file.json", "https://example.com/file2.json"]
    for file in files:
        httpx_mock.add_response(
            url=file,
            method="GET",
            json=SIMPLE_RECORD,
        )
    subject = RemoteFileExtractor(files)
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD, SIMPLE_RECORD]))


def test_file_ordereing():
    files_in_order = [Path(f"file{i}.json") for i in range(1, 4)]
    for permutation in itertools.permutations(files_in_order):
        subject = FileExtractor(permutation)
        assert_that(list(subject._ordered_paths()), equal_to(files_in_order))
