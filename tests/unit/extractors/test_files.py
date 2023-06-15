import csv
import json
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pytest
from hamcrest import assert_that, equal_to, has_length

from nodestream.extractors.files import FileExtractor

SIMPLE_RECORD = {"record": "value"}


@pytest.fixture
def fixture_directory():
    with TemporaryDirectory() as tempdir:
        yield tempdir


@pytest.fixture
def json_file(fixture_directory):
    with NamedTemporaryFile("w+", suffix=".json", dir=fixture_directory) as temp_file:
        json.dump(SIMPLE_RECORD, temp_file)
        temp_file.seek(0)
        yield Path(temp_file.name)


@pytest.fixture
def csv_file(fixture_directory):
    with NamedTemporaryFile("w+", suffix=".csv", dir=fixture_directory) as temp_file:
        writer = csv.DictWriter(temp_file, SIMPLE_RECORD.keys())
        writer.writeheader()
        writer.writerow(SIMPLE_RECORD)
        temp_file.seek(0)
        yield Path(temp_file.name)


@pytest.fixture
def txt_file(fixture_directory):
    with NamedTemporaryFile("w+", suffix=".txt", dir=fixture_directory) as temp_file:
        temp_file.write("hello world")
        temp_file.seek(0)
        yield Path(temp_file.name)


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


def test_declarative_init(fixture_directory, csv_file, json_file, txt_file):
    subject = FileExtractor.from_file_data(globs=[f"{fixture_directory}/**"])
    assert_that(list(subject.paths), has_length(3))
