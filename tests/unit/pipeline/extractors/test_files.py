import bz2
import csv
import gzip
import json
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pandas as pd
import pytest
import yaml
from hamcrest import (
    assert_that,
    contains_string,
    equal_to,
    has_items,
    has_key,
    has_length,
    not_,
)
from moto import mock_aws

from nodestream.pipeline.extractors.files import (
    FileExtractor,
    FileSource,
    LocalFileSource,
    RemoteFileSource,
)

SIMPLE_RECORD = {"record": "value"}


@pytest.fixture
def fixture_directory():
    with TemporaryDirectory() as tempdir:
        yield tempdir


@pytest.fixture
def unsupported_file(fixture_directory):
    with NamedTemporaryFile(
        "w+", suffix=".unsupported", dir=fixture_directory, delete=False
    ) as temp_file:
        name = temp_file.name
        temp_file.write("hello world")
        temp_file.seek(0)
    yield Path(name)


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
def empty_json_file(fixture_directory):
    with NamedTemporaryFile(
        "w+", suffix=".json", dir=fixture_directory, delete=False
    ) as temp_file:
        name = temp_file.name
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
def unformatted_jsonl_file(fixture_directory):
    with NamedTemporaryFile(
        "w+", suffix=".jsonl", dir=fixture_directory, delete=False
    ) as temp_file:
        name = temp_file.name
        writer = csv.DictWriter(temp_file, SIMPLE_RECORD.keys())
        writer.writeheader()
        writer.writerow(SIMPLE_RECORD)
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
    yield (path := Path(name))
    path.unlink(missing_ok=True)


@pytest.fixture
def yaml_file(fixture_directory):
    with NamedTemporaryFile(
        "w+", suffix=".yaml", dir=fixture_directory, delete=False
    ) as temp_file:
        name = temp_file.name
        yaml.dump(SIMPLE_RECORD, temp_file)
        temp_file.seek(0)
    yield Path(name)

@pytest.fixture
def incorrect_yaml_file(fixture_directory):
    with NamedTemporaryFile(
        "wb", suffix=".yaml", dir=fixture_directory, delete=False
    ) as temp_file:
        json_data = json.dumps(SIMPLE_RECORD).encode("utf-8")
        with gzip.GzipFile(fileobj=temp_file, mode="wb") as gzip_file:
            gzip_file.write(json_data)
        name = temp_file.name
    yield Path(name)


@pytest.fixture
def parquet_file(fixture_directory):
    with NamedTemporaryFile(
        "w+", suffix=".parquet", dir=fixture_directory, delete=False
    ) as temp_file:
        name = temp_file.name
        df = pd.DataFrame(data=SIMPLE_RECORD, index=[0])
        df.to_parquet(temp_file.name)
        temp_file.seek(0)
    yield Path(name)

@pytest.fixture
def incorrect_parquet_file(fixture_directory):
    with NamedTemporaryFile(
        "wb", suffix=".parquet", dir=fixture_directory, delete=False
    ) as temp_file:
        json_data = json.dumps(SIMPLE_RECORD).encode("utf-8")
        with gzip.GzipFile(fileobj=temp_file, mode="wb") as gzip_file:
            gzip_file.write(json_data)
        name = temp_file.name
    yield Path(name)


@pytest.fixture
def gzip_file(fixture_directory):
    with NamedTemporaryFile(
        "wb", suffix=".json.gz", dir=fixture_directory, delete=False
    ) as temp_file:
        json_data = json.dumps(SIMPLE_RECORD).encode("utf-8")
        with gzip.GzipFile(fileobj=temp_file, mode="wb") as gzip_file:
            gzip_file.write(json_data)
        name = temp_file.name
        if not temp_file.name.endswith(".json.gz"):
            raise Exception("not a json gzip file")
    yield Path(name)


@pytest.fixture
def bz2_file(fixture_directory):
    with NamedTemporaryFile(
        "wb", suffix=".json.bz2", dir=fixture_directory, delete=False
    ) as temp_file:
        json_data = json.dumps(SIMPLE_RECORD).encode("utf-8")
        name = temp_file.name
        with bz2.BZ2File(name, mode="wb") as bz2_file:
            bz2_file.write(json_data)
        if not temp_file.name.endswith(".json.bz2"):
            raise Exception("not a json bz2 file")
    yield Path(name)


@pytest.mark.asyncio
async def test_unsupported_file(unsupported_file):
    subject = FileExtractor([LocalFileSource([unsupported_file])])
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([]))


@pytest.mark.asyncio
async def test_supported_file_unified(unsupported_file):
    subject = FileExtractor.from_file_data(
        [{"type": "local", "globs": [str(unsupported_file)]}]
    )
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([]))


@pytest.mark.asyncio
async def test_json_formatting(json_file):
    subject = FileExtractor([LocalFileSource([json_file])])
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD]))


@pytest.mark.asyncio
async def test_json_formatting_empty_file(empty_json_file):
    subject = FileExtractor([LocalFileSource([empty_json_file])])
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([]))

@pytest.mark.asyncio
async def test_json_formatting_unified(json_file):
    subject = FileExtractor.from_file_data(
        [{"type": "local", "globs": [str(json_file)]}]
    )
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD]))


@pytest.mark.asyncio
async def test_csv_formatting(csv_file):
    subject = FileExtractor([LocalFileSource([csv_file])])
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD]))

@pytest.mark.asyncio
async def test_csv_formatting_unformatted_file(unformatted_csv_file):
    subject = FileExtractor([LocalFileSource([unformatted_csv_file])])
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([]))


@pytest.mark.asyncio
async def test_csv_formatting_unified(csv_file):
    subject = FileExtractor.from_file_data(
        [{"type": "local", "globs": [str(csv_file)]}]
    )
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD]))


@pytest.mark.asyncio
async def test_txt_formatting(txt_file):
    subject = FileExtractor([LocalFileSource([txt_file])])
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([{"line": "hello world"}]))


@pytest.mark.asyncio
async def test_txt_formatting_unified(txt_file):
    subject = FileExtractor.from_file_data(
        [{"type": "local", "globs": [str(txt_file)]}]
    )
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([{"line": "hello world"}]))


@pytest.mark.asyncio
async def test_jsonl_formatting(jsonl_file):
    subject = FileExtractor([LocalFileSource([jsonl_file])])
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD, SIMPLE_RECORD]))

@pytest.mark.asyncio
async def test_jsonl_formatting_unformatted_file(unformatted_jsonl_file):
    subject = FileExtractor([LocalFileSource([unformatted_jsonl_file])])
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([]))


@pytest.mark.asyncio
async def test_jsonl_formatting_unified(jsonl_file):
    subject = FileExtractor.from_file_data(
        [{"type": "local", "globs": [str(jsonl_file)]}]
    )
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD, SIMPLE_RECORD]))


@pytest.mark.asyncio
async def test_yaml_formatting(yaml_file):
    subject = FileExtractor([LocalFileSource([yaml_file])])
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD]))


@pytest.mark.asyncio
async def test_yaml_formatting_unformatted_file(incorrect_yaml_file):
    subject = FileExtractor([LocalFileSource([incorrect_yaml_file])])
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([]))


@pytest.mark.asyncio
async def test_yaml_formatting_unified(yaml_file):
    subject = FileExtractor.from_file_data(
        [{"type": "local", "globs": [str(yaml_file)]}]
    )
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD]))


@pytest.mark.asyncio
async def test_parquet_formatting(parquet_file):
    subject = FileExtractor([LocalFileSource([parquet_file])])
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD]))


@pytest.mark.asyncio
async def test_parquet_formatting_unformatted_file(incorrect_parquet_file):
    subject = FileExtractor([LocalFileSource([incorrect_parquet_file])])
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([]))


@pytest.mark.asyncio
async def test_parquet_formatting_unified(parquet_file):
    subject = FileExtractor.from_file_data(
        [{"type": "local", "globs": [str(parquet_file)]}]
    )
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD]))


@pytest.mark.asyncio
async def test_gzip_formatting(gzip_file):
    subject = FileExtractor([LocalFileSource([gzip_file])])
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD]))


@pytest.mark.asyncio
async def test_gzip_formatting_unified(gzip_file):
    subject = FileExtractor.from_file_data(
        [{"type": "local", "globs": [str(gzip_file)]}]
    )
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD]))


@pytest.mark.asyncio
async def test_bz2_formatting(bz2_file):
    subject = FileExtractor([LocalFileSource([bz2_file])])
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD]))


@pytest.mark.asyncio
async def test_bz2_formatting_unified(bz2_file):
    subject = FileExtractor.from_file_data(
        [{"type": "local", "globs": [str(bz2_file)]}]
    )
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD]))


def test_declarative_init(
    fixture_directory, csv_file, json_file, txt_file, jsonl_file, yaml_file
):
    subject = FileExtractor.local(globs=[f"{fixture_directory}/**"])
    assert_that(list(subject.file_sources[0].paths), has_length(5))


@pytest.mark.asyncio
async def test_remote_file_extractor_extract_records(mocker, httpx_mock):
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
    subject = FileExtractor([LocalFileSource([])])
    subject.logger = mocker.Mock()
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([]))
    subject.logger.warning.assert_called_once_with(
        "No files found for source: 0 local files"
    )


@pytest.mark.asyncio
async def test_no_files_found_from_remote_source(mocker):
    subject = FileExtractor([RemoteFileSource([], 10)])
    subject.logger = mocker.Mock()
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([]))
    subject.logger.warning.assert_called_once_with(
        "No files found for source: 0 remote files"
    )


def test_remote_file_source_single_file_description():
    url = "https://example.com/file.json"
    subject = RemoteFileSource([url], 10)
    assert_that(subject.describe(), equal_to(url))


def test_remote_file_source_multiple_file_description():
    urls = ["https://example.com/file.json", "https://example.com/file2.json"]
    subject = RemoteFileSource(urls, 10)
    assert_that(subject.describe(), equal_to("2 remote files"))


def test_local_file_source_single_file_description(fixture_directory):
    path = Path(f"{fixture_directory}/file.json")
    subject = LocalFileSource([path])
    assert_that(subject.describe(), equal_to(str(path)))


def test_local_file_source_multiple_file_description(fixture_directory):
    paths = [
        Path(f"{fixture_directory}/file.json"),
        Path(f"{fixture_directory}/file2.json"),
    ]
    subject = LocalFileSource(paths)
    assert_that(subject.describe(), equal_to("2 local files"))


def test_file_source_default_description():
    class SomeFileSource(FileSource):
        def get_files(self):
            pass

    subject = SomeFileSource()
    assert_that(subject.describe(), contains_string("SomeFileSource"))


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
            {"type": "local", "globs": [str(json_file)]},
            {"type": "local", "globs": [str(csv_file)]},
            {"type": "http", "urls": urls},
        ]
    )
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to([SIMPLE_RECORD] * 4))


BUCKET_NAME = "bucket"
PREFIX = "prefix"
NUM_OBJECTS = 10


@pytest.fixture
def s3_client():
    with mock_aws():
        import boto3

        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def subject(mocker):
    from nodestream.pipeline.extractors.credential_utils import AwsClientFactory

    mocker.patch.object(AwsClientFactory, "make_client")
    return FileExtractor.s3(bucket="bucket", prefix=PREFIX)


@pytest.fixture
def subject_with_populated_objects(subject, s3_client):
    subject.file_sources[0].s3_client = s3_client
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    s3_client.put_object(Bucket=BUCKET_NAME, Key="notprefixed", Body="hello".encode())
    for i in range(NUM_OBJECTS):
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{PREFIX}/foo/{i}.json",
            Body=json.dumps({"hello": i}),
        )
    return subject


@pytest.fixture
def subject_with_populated_objects_and_no_extension(subject, s3_client):
    subject.file_sources[0].s3_client = s3_client
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    s3_client.put_object(Bucket=BUCKET_NAME, Key="notprefixed", Body="hello".encode())
    for i in range(NUM_OBJECTS):
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{PREFIX}/foo/{i}.json",
            Body=json.dumps({"hello": i}),
        )
    return subject


@pytest.fixture
def subject_with_populated_csv_objects(subject, s3_client):
    subject.file_sources[0].s3_client = s3_client
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    s3_client.put_object(Bucket=BUCKET_NAME, Key="notprefixed", Body="hello".encode())
    for i in range(NUM_OBJECTS):
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{PREFIX}/bar/{i}.csv",
            Body=f"column1,column2\nvalue{i},value{i+10}",
        )
    return subject


@pytest.fixture
def subject_with_populated_jsonl_objects(subject, s3_client):
    subject.file_sources[0].s3_client = s3_client
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    for i in range(1):
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{PREFIX}/bar/{i}.jsonl",
            Body='{"test":"test"}\n{"test2":"test2"}',
        )
    return subject


@pytest.fixture
def subject_with_populated_text_objects(subject, s3_client):
    subject.file_sources[0].s3_client = s3_client
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    for i in range(1):
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{PREFIX}/bar/{i}.txt",
            Body="test\ntest2\n",
        )
    return subject


@pytest.fixture
def subject_with_archieved_objects(subject_with_archiving_enabled, s3_client):
    for i in range(NUM_OBJECTS):
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"archieve/{i}.json",
            Body=json.dumps({"hello": i}),
        )
    return subject_with_archiving_enabled


@pytest.fixture
def subject_with_populated_and_gz_compressed_objects(subject, s3_client):
    subject.file_sources[0].s3_client = s3_client
    s3_client.create_bucket(Bucket=BUCKET_NAME)

    for i in range(NUM_OBJECTS):
        body_data = json.dumps({"hello": i}).encode()
        gzipped_body = gzip.compress(body_data)

        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{PREFIX}/foo/{i}.json.gz",
            Body=gzipped_body,
        )

    return subject


@pytest.fixture
def subject_with_populated_and_bz2_compressed_objects(subject, s3_client):
    subject.file_sources[0].s3_client = s3_client
    s3_client.create_bucket(Bucket=BUCKET_NAME)

    for i in range(NUM_OBJECTS):
        body_data = json.dumps({"hello": i}).encode()
        bz2_body = bz2.compress(body_data)

        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{PREFIX}/foo/{i}.json.bz2",
            Body=bz2_body,
        )

    return subject


@pytest.fixture
def subject_with_populated_double_compressed_objects(subject, s3_client):
    subject.file_sources[0].s3_client = s3_client
    s3_client.create_bucket(Bucket=BUCKET_NAME)

    for i in range(NUM_OBJECTS):
        body_data = json.dumps({"hello": i}).encode()
        gzipped_body = gzip.compress(body_data)
        double_zipped_body = bz2.compress(gzipped_body)

        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{PREFIX}/foo/{i}.json.gz.bz2",
            Body=double_zipped_body,
        )

    return subject


@pytest.mark.asyncio
async def test_s3_extractor_properly_loads_csv_files(
    subject_with_populated_csv_objects,
):
    expected_results = [
        {"column1": f"value{i}", "column2": f"value{i+10}"} for i in range(NUM_OBJECTS)
    ]
    results = [
        result async for result in subject_with_populated_csv_objects.extract_records()
    ]
    assert_that(results, has_length(NUM_OBJECTS))
    assert_that(results, has_items(*expected_results))


@pytest.mark.asyncio
async def test_s3_extractor_properly_loads_jsonl_files(
    subject_with_populated_jsonl_objects,
):
    expected_results = [{"test": "test"}, {"test2": "test2"}]
    results = [
        result
        async for result in subject_with_populated_jsonl_objects.extract_records()
    ]
    assert_that(results, has_length(2))
    assert_that(results, has_items(*expected_results))


@pytest.mark.asyncio
async def test_s3_extractor_properly_loads_text_files(
    subject_with_populated_text_objects,
):
    expected_results = [{"line": "test"}, {"line": "test2"}]
    results = [
        result async for result in subject_with_populated_text_objects.extract_records()
    ]
    assert_that(results, has_length(2))
    assert_that(results, has_items(*expected_results))


@pytest.fixture
def subject_with_archiving_enabled(subject_with_populated_objects):
    subject_with_populated_objects.file_sources[0].archive_dir = "archive"
    return subject_with_populated_objects


@pytest.mark.asyncio
async def test_s3_extractor_pages_and_reads_all_files(subject_with_populated_objects):
    expected_results = [{"hello": i} for i in range(NUM_OBJECTS)]
    results = [
        result async for result in subject_with_populated_objects.extract_records()
    ]
    assert_that(results, has_length(NUM_OBJECTS))
    assert_that(results, has_items(*expected_results))


@pytest.mark.asyncio
async def test_s3_extractor_pages_and_reads_all_files_with_object_format(
    subject_with_populated_objects_and_no_extension,
):
    subject_with_populated_objects_and_no_extension.file_sources[0].object_format = (
        ".json"
    )
    expected_results = [{"hello": i} for i in range(NUM_OBJECTS)]
    results = [
        result
        async for result in subject_with_populated_objects_and_no_extension.extract_records()
    ]
    assert_that(results, has_length(NUM_OBJECTS))
    assert_that(results, has_items(*expected_results))


@pytest.mark.asyncio
async def test_s3_extractor_pages_and_reads_all_gz_compressed_files(
    subject_with_populated_and_gz_compressed_objects,
):
    expected_results = [{"hello": i} for i in range(NUM_OBJECTS)]
    results = [
        result
        async for result in subject_with_populated_and_gz_compressed_objects.extract_records()
    ]
    assert_that(results, has_length(NUM_OBJECTS))
    assert_that(results, has_items(*expected_results))


@pytest.mark.asyncio
async def test_s3_extractor_pages_and_reads_all_bz2_compressed_files(
    subject_with_populated_and_bz2_compressed_objects,
):
    expected_results = [{"hello": i} for i in range(NUM_OBJECTS)]
    results = [
        result
        async for result in subject_with_populated_and_bz2_compressed_objects.extract_records()
    ]
    assert_that(results, has_length(NUM_OBJECTS))
    assert_that(results, has_items(*expected_results))


@pytest.mark.asyncio
async def test_s3_extractor_pages_and_reads_double_compressed_files(
    subject_with_populated_double_compressed_objects,
):
    expected_results = [{"hello": i} for i in range(NUM_OBJECTS)]
    results = [
        result
        async for result in subject_with_populated_double_compressed_objects.extract_records()
    ]
    assert_that(results, has_length(NUM_OBJECTS))
    assert_that(results, has_items(*expected_results))


@pytest.mark.asyncio
async def test_s3_extractor_archives_objects(subject_with_archiving_enabled, s3_client):
    [result async for result in subject_with_archiving_enabled.extract_records()]
    assert_that(
        s3_client.list_objects(Bucket=BUCKET_NAME, Prefix="archive", MaxKeys=1000)[
            "Contents"
        ],
        has_length(NUM_OBJECTS),
    )


@pytest.mark.asyncio
async def test_s3_extractor_does_not_archive_objects_when_archive_dir_is_none(
    subject_with_populated_objects, s3_client
):
    [result async for result in subject_with_populated_objects.extract_records()]
    assert_that(
        s3_client.list_objects(Bucket=BUCKET_NAME, Prefix="archive", MaxKeys=1000),
        not_(has_key("Contents")),
    )
