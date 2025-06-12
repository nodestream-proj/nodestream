import bz2
import gzip
import json
from typing import Callable
from unittest.mock import MagicMock

import pytest
from hamcrest import assert_that, has_key, not_
from moto import mock_aws

from nodestream.pipeline.extractors import FileExtractor
from nodestream.pipeline.extractors.files import S3FileSource

# Commonly used test data
CSV_0 = {"column1": "value0", "column2": "value10"}
TXT_1 = {"line": "test1"}
TXT_0 = {"line": "test0"}
JSON_0 = {"hello": 0}
JSONL_1 = {"test": "test1"}
JSONL_0 = {"test": "test0"}

BUCKET_NAME = "bucket"
PREFIX = "prefix"
NUM_OBJECTS = 10


def _put_all(s3_client, data: list[tuple[str, Callable[[int], bytes]]]):
    for i, item in enumerate(data):
        ext, content_fn = item
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{PREFIX}/foo/{i}{ext}",
            Body=content_fn(i),
        )


def _gz_compress(fn: Callable[[int], bytes]) -> Callable[[int], bytes]:
    return lambda x: gzip.compress(fn(x))


def _bz2_compress(fn: Callable[[int], bytes]) -> Callable[[int], bytes]:
    return lambda x: bz2.compress(fn(x))


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
    # indent=2 makes the JSON multiline, which breaks jsonl parsing
    return json.dumps({"hello": i}, indent=2).encode("utf-8")


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
    expected_results = [JSONL_0, JSONL_1]
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


@pytest.mark.parametrize(
    "args,expected",
    [
        pytest.param(
            {"bucket": "test-bucket"},
            "S3FileSource{'bucket': 'test-bucket'}",
            id="bucket_only",
        ),
        pytest.param(
            {"bucket": "test-bucket", "archive_dir": "test-archive"},
            "S3FileSource{'bucket': 'test-bucket', 'archive_dir': 'test-archive'}",
            id="bucket_and_archive",
        ),
        pytest.param(
            {"bucket": "test-bucket", "prefix": "test-prefix"},
            "S3FileSource{'bucket': 'test-bucket', 'prefix': 'test-prefix'}",
            id="bucket_and_prefix",
        ),
        pytest.param(
            {
                "bucket": "test-bucket",
                "prefix": "test-prefix",
                "archive_dir": "test-archive",
            },
            "S3FileSource{'bucket': 'test-bucket', 'archive_dir': 'test-archive', 'prefix': 'test-prefix'}",
            id="bucket_and_prefix_and_archive",
        ),
        pytest.param(
            {"bucket": "test-bucket", "suffix": "test-suffix"},
            "S3FileSource{'bucket': 'test-bucket', 'suffix': 'test-suffix'}",
            id="bucket_and_suffix",
        ),
        pytest.param(
            {
                "bucket": "test-bucket",
                "suffix": "test-suffix",
                "archive_dir": "test-archive",
            },
            "S3FileSource{'bucket': 'test-bucket', 'archive_dir': 'test-archive', 'suffix': 'test-suffix'}",
            id="bucket_and_suffix_and_archive",
        ),
        pytest.param(
            {"bucket": "test-bucket", "object_format": "test-object_format"},
            "S3FileSource{'bucket': 'test-bucket', 'object_format': 'test-object_format'}",
            id="bucket_and_object_format",
        ),
        pytest.param(
            {
                "bucket": "test-bucket",
                "object_format": "test-object_format",
                "archive_dir": "test-archive",
            },
            "S3FileSource{'bucket': 'test-bucket', 'archive_dir': 'test-archive', 'object_format': 'test-object_format'}",
            id="bucket_and_object_format_and_archive",
        ),
        pytest.param(
            {
                "bucket": "test-bucket",
                "prefix": "test-prefix",
                "suffix": "test-suffix",
                "object_format": "test-object_format",
                "archive_dir": "test-archive",
            },
            "S3FileSource{'bucket': 'test-bucket', 'archive_dir': 'test-archive', 'object_format': 'test-object_format', 'prefix': 'test-prefix', 'suffix': 'test-suffix'}",
            id="everything",
        ),
    ],
)
def test_s3_filesource_describe(args, expected):
    assert S3FileSource(s3_client=MagicMock(), **args).describe() == expected


FILTER_TEST_DATA = [
    pytest.param(
        ".jsonl",
        _jsonl_file,
        [JSONL_0, JSONL_1],
        ["s3://bucket/prefix/bar/filename.jsonl"],
        id="jsonl_extension_jsonl_contents",
    ),
    pytest.param(
        ".jsonl",
        _json_file,
        [],
        ["s3://bucket/prefix/bar/filename.jsonl"],
        id="jsonl_extension_json_contents",
    ),
    pytest.param(
        ".jsonl.gz",
        _gz_compress(_json_file),
        [],
        [],
        id="wrong_extension_gz",
    ),
    pytest.param(
        ".jsonl.bz2",
        _bz2_compress(_json_file),
        [],
        [],
        id="wrong_extension_bz2",
    ),
    pytest.param(".txt", _text_file, [], [], id="wrong_extension_txt"),
    pytest.param(".csv", _jsonl_file, [], [], id="wrong_extension_csv"),
    pytest.param("", _jsonl_file, [], [], id="no_extension"),
]


@pytest.mark.parametrize(
    "file_extension, contents, expected, expected_s3_files",
    FILTER_TEST_DATA,
)
@pytest.mark.parametrize(
    "filter_arg",
    [{"object_format": ".jsonl"}, {"suffix": ".jsonl"}],
    ids=lambda d: next(k for k in d),
)  # suffix and object_format on their own should act in an identical way
@pytest.mark.asyncio
async def test_s3_should_filter(
    s3_client,
    file_extension,
    contents,
    expected,
    expected_s3_files,
    filter_arg,
):
    key = f"{PREFIX}/bar/filename{file_extension}"
    s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=contents(0))
    subject = FileExtractor.s3(bucket=BUCKET_NAME, prefix=PREFIX, **filter_arg)

    sources = subject.file_sources
    assert sources == [
        S3FileSource(
            s3_client=s3_client,
            bucket=BUCKET_NAME,
            prefix=PREFIX,
            **filter_arg,
        )
    ]

    assert [str(f) async for f in sources[0].get_files()] == expected_s3_files
    assert [result async for result in subject.extract_records()] == expected


@pytest.mark.parametrize(
    "file_extension, contents, expected, expected_s3_files",
    [
        pytest.param(
            ".json.gz",
            _gz_compress(_jsonl_file),
            [JSONL_0, JSONL_1],
            ["s3://bucket/prefix/bar/filename.json.gz"],
            id="jsonl_extension_jsonl_contents",
        ),
        pytest.param(
            ".json.gz",
            _gz_compress(_json_file),
            [],
            ["s3://bucket/prefix/bar/filename.json.gz"],
            id="jsonl_extension_json_contents",
        ),
        pytest.param(
            ".json.gz",
            _json_file,
            [],
            ["s3://bucket/prefix/bar/filename.json.gz"],
            id="not_gzipped",
        ),
        pytest.param(
            ".json.gz",
            _bz2_compress(_json_file),
            [],
            ["s3://bucket/prefix/bar/filename.json.gz"],
            id="unexpected_bz2",
        ),
        pytest.param(".json", _json_file, [], [], id="wrong_suffix"),
    ],
)
@pytest.mark.asyncio
async def test_s3_should_filter_by_suffix_and_process_by_object_format(
    s3_client,
    file_extension,
    contents,
    expected,
    expected_s3_files,
):
    key = f"{PREFIX}/bar/filename{file_extension}"
    s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=contents(0))
    subject = FileExtractor.s3(
        bucket=BUCKET_NAME, prefix=PREFIX, object_format=".jsonl.gz", suffix=".json.gz"
    )

    sources = subject.file_sources
    assert sources == [
        S3FileSource(
            s3_client=s3_client,
            bucket=BUCKET_NAME,
            prefix=PREFIX,
            object_format=".jsonl.gz",
            suffix=".json.gz",
        )
    ]

    assert [str(f) async for f in sources[0].get_files()] == expected_s3_files

    assert [result async for result in subject.extract_records()] == expected


@pytest.mark.asyncio
async def test_s3_recursive_after_suffix_filter(s3_client):

    _put_all(
        s3_client,
        [
            (".json.gz", _gz_compress(_json_file)),
            (".jsonl.gz", _gz_compress(_jsonl_file)),
            (".csv.gz", _gz_compress(_csv_file)),
            (".txt.bz2.gz.gz", _gz_compress(_gz_compress(_bz2_compress(_text_file)))),
        ],
    )

    subject = FileExtractor.s3(bucket=BUCKET_NAME, prefix=PREFIX, suffix=".gz")
    sources = subject.file_sources
    assert sources == [
        S3FileSource(
            s3_client=s3_client,
            bucket=BUCKET_NAME,
            prefix=PREFIX,
            suffix=".gz",
        )
    ]

    assert [str(f) async for f in sources[0].get_files()] == [
        "s3://bucket/prefix/foo/0.json.gz",
        "s3://bucket/prefix/foo/1.jsonl.gz",
        "s3://bucket/prefix/foo/2.csv.gz",
        "s3://bucket/prefix/foo/3.txt.bz2.gz.gz",
    ]

    assert [result async for result in subject.extract_records()] == [
        JSON_0,
        {"test": "test2"},
        {"test": "test3"},
        {"column1": "value2", "column2": "value12"},
        {"line": "test6"},
        {"line": "test7"},
    ]


@pytest.mark.parametrize(
    "file_extension, contents, expected",
    [
        pytest.param(".jsonl", _jsonl_file, [JSONL_0, JSONL_1], id="jsonl"),
        pytest.param(
            ".jsonl.gz", _gz_compress(_jsonl_file), [JSONL_0, JSONL_1], id="jsonl_gz"
        ),
        pytest.param(".json.bz2", _bz2_compress(_json_file), [JSON_0], id="json_bz2"),
        pytest.param(
            ".jsonl.bz2.gz",
            _gz_compress(_bz2_compress(_jsonl_file)),
            [JSONL_0, JSONL_1],
            id="jsonl_bz2_gz",
        ),
        pytest.param(
            ".txt.gz.bz2",
            _bz2_compress(_gz_compress(_text_file)),
            [TXT_0, TXT_1],
            id="txt_gz_bz2",
        ),
        pytest.param(
            ".txt.gz.gz.gz.gz.gz",
            _gz_compress(
                _gz_compress(_gz_compress(_gz_compress(_gz_compress(_text_file))))
            ),
            [TXT_0, TXT_1],
            id="gz_insanity",
        ),
        pytest.param(".txt", _text_file, [TXT_0, TXT_1], id="txt"),
        pytest.param(".csv.gz", _gz_compress(_csv_file), [CSV_0], id="csv_gz"),
        pytest.param(".csv.bz2", _bz2_compress(_csv_file), [CSV_0], id="csv_bz2"),
        pytest.param(".json.gz", _gz_compress(_json_file), [JSON_0], id="json_gz"),
        pytest.param("", _json_file, [], id="no_extension"),
        pytest.param(".json.bz2", _gz_compress(_json_file), [], id="not_bzipped"),
        pytest.param(".json.gz", _bz2_compress(_json_file), [], id="not_gzipped"),
    ],
)
@pytest.mark.asyncio
async def test_s3_no_object_format_no_suffix_process_by_extension(
    s3_client,
    file_extension,
    contents,
    expected,
):
    key = f"{PREFIX}/bar/filename{file_extension}"
    s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=contents(0))
    subject = FileExtractor.s3(bucket=BUCKET_NAME, prefix=PREFIX)

    assert subject.file_sources == [
        S3FileSource(
            s3_client=s3_client,
            bucket=BUCKET_NAME,
            prefix=PREFIX,
        )
    ]

    assert [str(f) async for f in subject.file_sources[0].get_files()] == [
        f"s3://{BUCKET_NAME}/{key}",
    ]

    assert [result async for result in subject.extract_records()] == expected


@pytest.mark.asyncio
async def test_s3_get_all_via_blank_suffix_process_with_object_format(s3_client):
    _put_all(
        s3_client,
        [
            (".json", _json_file),
            (".jsonl", _json_file),
            (".txt", _text_file),
            (".json", _json_file),
        ],
    )

    subject = FileExtractor.s3(
        bucket=BUCKET_NAME,
        prefix=PREFIX,
        suffix="",
        object_format=".json",
    )
    sources = subject.file_sources
    assert sources == [
        S3FileSource(
            s3_client=s3_client,
            bucket=BUCKET_NAME,
            prefix=PREFIX,
            suffix="",
            object_format=".json",
        )
    ]

    assert [str(f) async for f in sources[0].get_files()] == [
        "s3://bucket/prefix/foo/0.json",
        "s3://bucket/prefix/foo/1.jsonl",
        "s3://bucket/prefix/foo/2.txt",
        "s3://bucket/prefix/foo/3.json",
    ]

    assert [result async for result in subject.extract_records()] == [
        JSON_0,
        {"hello": 1},
        {"hello": 3},
    ]
