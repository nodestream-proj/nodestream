import bz2
import gzip
import json
import logging
from unittest.mock import MagicMock

import pytest
from hamcrest import assert_that, has_key, not_
from moto import mock_aws

from nodestream.pipeline.extractors import FileExtractor
from nodestream.pipeline.extractors.files import S3FileSource

BUCKET_NAME = "bucket"
PREFIX = "prefix"
NUM_OBJECTS = 10

log = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def patch_make_client(mocker, s3_client):
    """Ensures that the S3 client is mocked for all tests.

    Note: `autouse=True` is required to automatically apply this fixture to all
    tests in the module.
    """
    from nodestream.pipeline.extractors.credential_utils import AwsClientFactory

    mocker.patch.object(AwsClientFactory, "make_client", return_value=s3_client)


@pytest.fixture
def s3_client():
    with mock_aws():
        import boto3

        s3 = boto3.client("s3", region_name="us-east-1")
        # Create a bucket for testing
        s3.create_bucket(Bucket=BUCKET_NAME)
        # Add a file that will never be found when using the PREFIX constant
        s3.put_object(Bucket=BUCKET_NAME, Key="notprefixed/file.txt", Body=b"hello")

        yield s3


def _csv_file(i: int) -> bytes:
    return f"column1,column2\nvalue{i},value{i+10}".encode("utf-8")


def _json_file(i: int):
    return json.dumps({"hello": i}).encode("utf-8")


def _jsonl_file(file_id: int, line_count: int = 2) -> bytes:
    lines = [
        json.dumps({"test": f"test{file_id * line_count + j}"})
        for j in range(line_count)
    ]
    return "\n".join(lines).encode("utf-8")


def _text_file(file_id: int, line_count: int = 2) -> bytes:
    lines = [
        f"test{file_id * line_count + j}"  # Simple text lines
        for j in range(line_count)
    ]
    return "\n".join(lines).encode("ascii")


def add_csv_objects(s3_client, count: int = NUM_OBJECTS):
    for i in range(count):
        key = f"{PREFIX}/foo/{i}.csv"
        body = _csv_file(i)
        s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=body)


def add_json_objects(s3_client, count: int = NUM_OBJECTS):
    for i in range(count):
        key = f"{PREFIX}/foo/{i}.json"
        body = _json_file(i)
        s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=body)


def add_jsonl_objects(s3_client, count: int = NUM_OBJECTS):
    for i in range(count):
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{PREFIX}/foo/{i}.jsonl",
            Body=_jsonl_file(i),
        )


def add_text_objects(s3_client, count: int = NUM_OBJECTS, lines: int = 2):
    for i in range(count):
        s3_client.put_object(
            Bucket=BUCKET_NAME, Key=f"{PREFIX}/bar/{i}.txt", Body=_text_file(i, lines)
        )


def add_archived_objects(s3_client, count: int = NUM_OBJECTS):
    for i in range(count):
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"archive/{i}.json",
            Body=_json_file(i),
        )


def add_gzipped_json_objects(s3_client, count: int = NUM_OBJECTS):
    for i in range(count):
        body_data = _json_file(i)
        gzipped_body = gzip.compress(body_data)

        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{PREFIX}/foo/{i}.json.gz",
            Body=gzipped_body,
        )


def add_bz2_compressed_json_objects(s3_client, count: int = NUM_OBJECTS):
    for i in range(count):
        body_data = _json_file(i)
        bz2_body = bz2.compress(body_data)

        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{PREFIX}/foo/{i}.json.bz2",
            Body=bz2_body,
        )


def add_double_compressed_json_objects(
    s3_client,
    *,
    prefix=f"{PREFIX}/foo/",
    count: int = NUM_OBJECTS,
):
    for i in range(count):
        body_data = _json_file(i)
        gzipped_body = gzip.compress(body_data)
        double_zipped_body = bz2.compress(gzipped_body)

        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{prefix}{i}.json.gz.bz2",
            Body=double_zipped_body,
        )


@pytest.mark.asyncio
async def test_s3_extractor_properly_loads_csv_files(s3_client):

    add_csv_objects(s3_client)
    expected_results = [
        {"column1": f"value{i}", "column2": f"value{i+10}"} for i in range(NUM_OBJECTS)
    ]
    subject = FileExtractor.s3(bucket=BUCKET_NAME, prefix=PREFIX)
    results = [result async for result in subject.extract_records()]

    assert results == expected_results


@pytest.mark.asyncio
async def test_s3_extractor_properly_loads_jsonl_files(s3_client):

    add_jsonl_objects(s3_client, 1)

    subject = FileExtractor.s3(bucket=BUCKET_NAME, prefix=PREFIX)
    expected_results = [{"test": "test0"}, {"test": "test1"}]
    results = [result async for result in subject.extract_records()]

    assert results == expected_results


@pytest.mark.asyncio
async def test_s3_extractor_properly_loads_text_files(s3_client):
    n_lines = 2
    add_text_objects(s3_client, lines=n_lines)
    subject = FileExtractor.s3(bucket=BUCKET_NAME, prefix=PREFIX)
    assert [result async for result in subject.extract_records()] == [
        {"line": f"test{i}"} for i in range(NUM_OBJECTS * n_lines)
    ]


@pytest.mark.asyncio
async def test_s3_extractor_pages_and_reads_all_files(s3_client):
    subject = FileExtractor.s3(bucket=BUCKET_NAME, prefix=PREFIX)
    add_json_objects(s3_client)
    expected = [{"hello": i} for i in range(NUM_OBJECTS)]
    assert [result async for result in subject.extract_records()] == expected


@pytest.mark.asyncio
async def test_s3_extractor_pages_and_reads_all_files_with_object_format(s3_client):
    subject = FileExtractor.s3(bucket=BUCKET_NAME, prefix=PREFIX, object_format=".json")
    s3_client.put_object(Bucket=BUCKET_NAME, Key="notprefixed", Body=b"hello")
    add_json_objects(s3_client)
    expected_results = [{"hello": i} for i in range(NUM_OBJECTS)]
    results = [result async for result in subject.extract_records()]

    assert results == expected_results


@pytest.mark.asyncio
async def test_s3_extractor_pages_and_reads_all_gz_compressed_files(s3_client):
    subject = FileExtractor.s3(bucket=BUCKET_NAME, prefix=PREFIX)
    add_gzipped_json_objects(s3_client)
    expected_results = [{"hello": i} for i in range(NUM_OBJECTS)]
    results = [result async for result in subject.extract_records()]

    assert results == expected_results


@pytest.mark.asyncio
async def test_s3_extractor_pages_and_reads_all_bz2_compressed_files(s3_client):
    add_bz2_compressed_json_objects(s3_client)

    subject = FileExtractor.s3(bucket=BUCKET_NAME, prefix=PREFIX)
    results = [result async for result in subject.extract_records()]

    assert results == [{"hello": i} for i in range(NUM_OBJECTS)]


@pytest.mark.asyncio
async def test_s3_extractor_pages_and_reads_double_compressed_files(s3_client):
    add_double_compressed_json_objects(s3_client)
    subject = FileExtractor.s3(bucket=BUCKET_NAME, prefix=PREFIX)
    assert [result async for result in subject.extract_records()] == [
        {"hello": i} for i in range(NUM_OBJECTS)
    ]


@pytest.mark.asyncio
async def test_s3_extractor_archives_objects(s3_client):
    s3_client.put_object(Bucket=BUCKET_NAME, Key="notprefixed", Body=b"hello")
    add_json_objects(s3_client, NUM_OBJECTS)
    subject = FileExtractor.s3(bucket=BUCKET_NAME, prefix=PREFIX, archive_dir="archive")
    assert [result async for result in subject.extract_records()] == [
        {"hello": i} for i in range(NUM_OBJECTS)
    ]

    response = s3_client.list_objects(
        Bucket=BUCKET_NAME,
        Prefix="archive",
        MaxKeys=1000,
    )
    expected = {f"archive/{i}.json" for i in range(NUM_OBJECTS)}
    assert {item["Key"] for item in response["Contents"]} == expected


@pytest.mark.asyncio
async def test_s3_extractor_does_not_archive_objects_when_archive_dir_is_none(
    s3_client,
):
    s3_client.put_object(Bucket=BUCKET_NAME, Key="notprefixed", Body=b"hello")
    s3_client.put_object(
        Bucket=BUCKET_NAME, Key=f"{PREFIX}/foo/1.json", Body=b'{"hello": 1}'
    )
    subject = FileExtractor.s3(bucket=BUCKET_NAME, prefix=PREFIX)
    assert [result async for result in subject.extract_records()] == [{"hello": 1}]
    assert_that(
        s3_client.list_objects(Bucket=BUCKET_NAME, Prefix="archive", MaxKeys=1000),
        not_(has_key("Contents")),
    )


def test_s3_filesource_describe():
    assert (
        S3FileSource(
            bucket="test-bucket",
            prefix="test-prefix",
            object_format=".json",
            archive_dir="archive",
            s3_client=MagicMock(),
        ).describe()
        == "S3FileSource{bucket: test-bucket, prefix: test-prefix, "
        "archive_dir: archive, object_format: .json}"
    )


@pytest.mark.parametrize(
    "compress_extension,compression_fn",
    [(".gz", gzip.compress), (".bz2", bz2.compress)],
)
@pytest.mark.asyncio
async def test_s3_should_treat_all_files_as_object_type(
    s3_client,
    compress_extension,
    compression_fn,
):

    await _object_type_bucket_setup(s3_client, compress_extension, compression_fn)

    subject = FileExtractor.s3(
        bucket=BUCKET_NAME, prefix=PREFIX, object_format=f".jsonl{compress_extension}"
    )

    assert subject.file_sources == [
        S3FileSource(
            s3_client=s3_client,
            bucket=BUCKET_NAME,
            prefix=PREFIX,
            object_format=f".jsonl{compress_extension}",
        )
    ]

    assert [str(f) async for f in subject.file_sources[0].get_files()] == [
        f"s3://bucket/prefix/bar/1.jsonl{compress_extension}",
        f"s3://bucket/prefix/bar/2.json{compress_extension}",
        "s3://bucket/prefix/bar/3.txt",
        f"s3://bucket/prefix/bar/4.csv{compress_extension}",
        f"s3://bucket/prefix/bar/5.json{compress_extension}",
        "s3://bucket/prefix/bar/6",
    ]

    assert [result async for result in subject.extract_records()] == [
        {"test": "test0"},
        {"test": "test1"},
        {"test": "test2"},
        {"test": "test3"},
        {"test": "test10"},
        {"test": "test11"},
    ]


@pytest.mark.parametrize(
    "compress_extension,compression_fn",
    [(".gz", gzip.compress), (".bz2", bz2.compress)],
)
@pytest.mark.asyncio
async def test_s3_should_handle_no_object_type(
    s3_client,
    compress_extension,
    compression_fn,
):
    await _object_type_bucket_setup(s3_client, compress_extension, compression_fn)
    subject = FileExtractor.s3(bucket=BUCKET_NAME, prefix=PREFIX)

    assert subject.file_sources == [
        S3FileSource(
            s3_client=s3_client,
            bucket=BUCKET_NAME,
            prefix=PREFIX,
        )
    ]

    assert [str(f) async for f in subject.file_sources[0].get_files()] == [
        f"s3://bucket/prefix/bar/1.jsonl{compress_extension}",
        f"s3://bucket/prefix/bar/2.json{compress_extension}",
        "s3://bucket/prefix/bar/3.txt",
        f"s3://bucket/prefix/bar/4.csv{compress_extension}",
        f"s3://bucket/prefix/bar/5.json{compress_extension}",
        "s3://bucket/prefix/bar/6",
    ]

    assert [result async for result in subject.extract_records()] == [
        {"test": "test0"},
        {"test": "test1"},
        {"column1": "value3", "column2": "value13"},
    ]


async def _object_type_bucket_setup(s3_client, compress_extension, compression_fn):
    # matching extension, matching data
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=f"{PREFIX}/bar/1.jsonl{compress_extension}",
        Body=compression_fn(_jsonl_file(0)),
    )
    # mismatched extension, matching data
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=f"{PREFIX}/bar/2.json{compress_extension}",
        Body=compression_fn(_jsonl_file(1)),
    )
    # mismatched extension, mismatched data
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=f"{PREFIX}/bar/3.txt",
        Body=compression_fn(_text_file(2)),
    )
    # mismatched extension, mismatched data
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=f"{PREFIX}/bar/4.csv{compress_extension}",
        Body=compression_fn(_csv_file(3)),
    )
    # mismatched extension, matching data (uncompressed)
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=f"{PREFIX}/bar/5.json{compress_extension}",
        Body=_jsonl_file(4),
    )
    # no extension, matching data
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=f"{PREFIX}/bar/6",
        Body=compression_fn(_jsonl_file(5)),
    )
