import bz2
import csv
import gzip
import json
import logging
import typing
import zipfile
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import AsyncIterator

import pandas as pd
import pytest
import yaml
from hamcrest import assert_that, equal_to

from nodestream.pipeline.extractors.files import (
    SUPPORTED_ARCHIVE_FORMAT_REGISTRY,
    SUPPORTED_FILE_FORMAT_REGISTRY,
    FileCodec,
    FileExtractor,
    FileSource,
    LocalFileSource,
    ReadableFile,
    RemoteFileSource,
    Sas7bdatFileFormat,
    ZipArchive,
    _is_data_member,
    _normalize_frame,
)

SIMPLE_RECORD = {"record": "value"}


def _warning_message(logger) -> str:
    logger.warning.assert_called_once()
    args = logger.warning.call_args.args
    return args[0] % args[1:]


# sample.sas7bdat is pandas' test1.sas7bdat; see tests/data/LICENSE-pandas.
SAS7BDAT_FIXTURE = Path(__file__).resolve().parents[3] / "data" / "sample.sas7bdat"
SAS_PREVIEW_COLUMNS = ("Column2", "Column3", "Column4", "Column8")
SAS_PREVIEW = [
    {
        "Column2": "pear",
        "Column3": 84.0,
        "Column4": datetime(1965, 12, 10),
        "Column8": None,
    },
    {
        "Column2": "dog",
        "Column3": 49.0,
        "Column4": datetime(1977, 3, 7),
        "Column8": 339.0,
    },
]


@pytest.fixture(autouse=True)
def base_dir():
    with TemporaryDirectory() as tempdir:
        yield tempdir


@pytest.fixture(autouse=True)
def unsupported_file(base_dir: str):
    with NamedTemporaryFile(
        "w+",
        suffix=".unsupported",
        dir=base_dir,
        delete=False,
    ) as temp_file:
        temp_file.write("hello world")
        temp_file.seek(0)
        yield temp_file


@pytest.fixture(autouse=True)
def json_file(base_dir: str):
    with NamedTemporaryFile(
        "w+",
        suffix=".json",
        dir=base_dir,
        delete=False,
    ) as temp_file:
        json.dump(SIMPLE_RECORD, temp_file)
        temp_file.seek(0)
        yield temp_file


@pytest.fixture
def empty_json_file(base_dir: str):
    with NamedTemporaryFile(
        "w+",
        suffix=".json",
        dir=base_dir,
        delete=False,
    ) as temp_file:
        yield temp_file


@pytest.fixture(autouse=True)
def jsonl_file(base_dir: str):
    with NamedTemporaryFile(
        "w+",
        suffix=".jsonl",
        dir=base_dir,
        delete=False,
    ) as temp_file:
        json.dump(SIMPLE_RECORD, temp_file)
        temp_file.write("\n")
        json.dump(SIMPLE_RECORD, temp_file)
        temp_file.seek(0)
        yield temp_file


@pytest.fixture(autouse=True)
def csv_file(base_dir: str):
    with NamedTemporaryFile(
        "w+",
        suffix=".csv",
        dir=base_dir,
        delete=False,
    ) as temp_file:
        writer = csv.DictWriter(temp_file, SIMPLE_RECORD.keys())
        writer.writeheader()
        writer.writerow(SIMPLE_RECORD)
        temp_file.seek(0)
        yield temp_file


@pytest.fixture(autouse=True)
def txt_file(base_dir: str):
    with NamedTemporaryFile(
        "w+",
        suffix=".txt",
        dir=base_dir,
        delete=False,
    ) as temp_file:
        temp_file.write("hello world")
        temp_file.seek(0)
        yield temp_file


@pytest.fixture(autouse=True)
def yaml_file(base_dir: str):
    with NamedTemporaryFile(
        "w+",
        suffix=".yaml",
        dir=base_dir,
        delete=False,
    ) as temp_file:
        yaml.dump(SIMPLE_RECORD, temp_file)
        temp_file.seek(0)
        yield temp_file


@pytest.fixture
def sas7bdat_file(base_dir: str):
    dest = Path(base_dir) / "sample.sas7bdat"
    dest.write_bytes(SAS7BDAT_FIXTURE.read_bytes())
    return dest


@pytest.fixture(autouse=True)
def parquet_file(base_dir: str):
    with NamedTemporaryFile(
        "wb",
        suffix=".parquet",
        dir=base_dir,
        delete=False,
    ) as temp_file:
        df = pd.DataFrame(data=SIMPLE_RECORD, index=[0])
        temp_file.write(df.to_parquet())
        temp_file.seek(0)
        yield temp_file


@pytest.fixture(autouse=True)
def gzip_file(base_dir: str):
    with NamedTemporaryFile(
        "wb",
        suffix=".json.gz",
        dir=base_dir,
        delete=False,
    ) as temp_file:
        json_data = json.dumps(SIMPLE_RECORD).encode("utf-8")
        temp_file.write(gzip.compress(json_data))
        temp_file.seek(0)
        yield temp_file


@pytest.fixture(autouse=True)
def bz2_file(base_dir: str):
    with NamedTemporaryFile(
        "wb",
        suffix=".json.bz2",
        dir=base_dir,
        delete=False,
    ) as temp_file:
        json_data = json.dumps(SIMPLE_RECORD).encode("utf-8")
        temp_file.write(bz2.compress(json_data))
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


def _sas_preview(records):
    return [
        {column: record[column] for column in SAS_PREVIEW_COLUMNS}
        for record in records[:2]
    ]


async def _records_from(path: Path):
    subject = FileExtractor([LocalFileSource([path])])
    return [record async for record in subject.extract_records()]


def _assert_sas_records(records):
    assert len(records) == 10
    assert all(len(record) == 100 for record in records)
    assert _sas_preview(records) == SAS_PREVIEW
    assert all(
        record["Column1"] is None or isinstance(record["Column1"], float)
        for record in records
    )
    assert records[0]["Column1"] == pytest.approx(0.636)
    assert all(
        record["Column3"] is None or isinstance(record["Column3"], float)
        for record in records
    )


@pytest.mark.asyncio
async def test_sas7bdat_file(sas7bdat_file: Path):
    _assert_sas_records(await _records_from(sas7bdat_file))


@pytest.mark.asyncio
async def test_uppercase_sas7bdat_suffix(base_dir: str, sas7bdat_file: Path):
    path = Path(base_dir) / "PEOPLE.SAS7BDAT"
    path.write_bytes(sas7bdat_file.read_bytes())
    _assert_sas_records(await _records_from(path))


@pytest.mark.asyncio
async def test_gzip_compressed_sas7bdat(base_dir: str, sas7bdat_file: Path):
    path = Path(base_dir) / "people.sas7bdat.gz"
    with gzip.open(path, "wb") as compressed:
        compressed.write(sas7bdat_file.read_bytes())
    _assert_sas_records(await _records_from(path))


@pytest.mark.asyncio
async def test_bz2_compressed_sas7bdat(base_dir: str, sas7bdat_file: Path):
    path = Path(base_dir) / "people.sas7bdat.bz2"
    with bz2.open(path, "wb") as compressed:
        compressed.write(sas7bdat_file.read_bytes())
    _assert_sas_records(await _records_from(path))


@pytest.mark.asyncio
async def test_zip_archive_dispatches_each_member(base_dir: str, sas7bdat_file: Path):
    archive_path = Path(base_dir) / "bundle.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.write(sas7bdat_file, "tables/one.sas7bdat")
        archive.writestr("notes/readme.txt", "ignore me")
        archive.writestr("notes/people.csv", "record\nvalue\n")
        archive.writestr("__MACOSX/._one.sas7bdat", b"not a sas file")
        archive.write(sas7bdat_file, "tables/two.SAS7BDAT")
    records = await _records_from(archive_path)
    _assert_sas_records(records[:10])
    assert records[10] == {"line": "ignore me"}
    assert records[11] == SIMPLE_RECORD
    _assert_sas_records(records[12:])


@pytest.mark.asyncio
async def test_zip_reads_csv_and_skips_directories(base_dir: str):
    archive_path = Path(base_dir) / "notes.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("notes.csv", "record\nvalue\n")
        archive.writestr("folder/", b"")
    assert await _records_from(archive_path) == [SIMPLE_RECORD]


def _corrupt_zip_member_crc(path: Path, member: str) -> None:
    """Flip one stored payload byte so the member's CRC no longer matches."""
    with zipfile.ZipFile(path) as archive:
        offset = archive.getinfo(member).header_offset
    raw = bytearray(path.read_bytes())
    name_len = int.from_bytes(raw[offset + 26 : offset + 28], "little")
    extra_len = int.from_bytes(raw[offset + 28 : offset + 30], "little")
    data_start = offset + 30 + name_len + extra_len
    raw[data_start] ^= 0xFF
    path.write_bytes(raw)


@pytest.mark.asyncio
async def test_zip_bad_member_does_not_drop_later_members(
    base_dir: str, sas7bdat_file: Path, mocker, caplog
):
    archive_path = Path(base_dir) / "bundle.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("a_corrupt.csv", "record\nvalue\n")
        archive.writestr("a_bad.sas7bdat", b"this is not a sas7bdat file")
        archive.write(sas7bdat_file, "b_good.sas7bdat")
    _corrupt_zip_member_crc(archive_path, "a_corrupt.csv")
    subject = FileExtractor([LocalFileSource([archive_path])])
    subject.logger = mocker.Mock()
    with caplog.at_level(
        logging.WARNING, logger="nodestream.pipeline.extractors.files"
    ):
        records = [record async for record in subject.extract_records()]
    _assert_sas_records(records)
    warning = " ".join(
        str(arg) for call in subject.logger.warning.call_args_list for arg in call.args
    )
    assert "a_bad.sas7bdat" in warning
    assert "a_corrupt.csv" in caplog.text
    assert "b_good.sas7bdat" not in warning
    assert "b_good.sas7bdat" not in caplog.text


@pytest.mark.asyncio
async def test_zip_skips_oversized_members(
    base_dir: str, sas7bdat_file: Path, monkeypatch, caplog
):
    limit = 32
    monkeypatch.setattr(ZipArchive, "max_member_bytes", limit)
    archive_path = Path(base_dir) / "bundle.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.write(sas7bdat_file, "big.sas7bdat")
        archive.writestr("notes.csv", "record\nvalue\n")
    with caplog.at_level(
        logging.WARNING, logger="nodestream.pipeline.extractors.files"
    ):
        assert await _records_from(archive_path) == [SIMPLE_RECORD]
    size_gb = f"{sas7bdat_file.stat().st_size / 1024**3:.1f} GB"
    limit_gb = f"{limit / 1024**3:.1f} GB"
    assert (
        "Skipping oversized member big.sas7bdat in "
        f"{archive_path} ({size_gb} > {limit_gb} limit, ZipArchive.max_member_bytes)"
    ) in caplog.text


@pytest.mark.asyncio
async def test_invalid_zip_logs_the_unpack_error(base_dir: str, mocker):
    path = Path(base_dir) / "bad.zip"
    path.write_bytes(b"this is not a zip file")
    subject = FileExtractor([LocalFileSource([path])])
    subject.logger = mocker.Mock()
    assert [record async for record in subject.extract_records()] == []
    message = _warning_message(subject.logger)
    assert message.startswith(f"Failed to read archive {path}.")
    assert message.endswith("File is not a zip file")


@pytest.mark.asyncio
async def test_invalid_sas7bdat_is_skipped(base_dir: str, mocker):
    path = Path(base_dir) / "bad.sas7bdat"
    path.write_bytes(b"this is not a sas7bdat file")
    subject = FileExtractor([LocalFileSource([path])])
    subject.logger = mocker.Mock()
    assert [record async for record in subject.extract_records()] == []
    message = _warning_message(subject.logger)
    reason = subject.logger.warning.call_args.kwargs["extra"]["exception"]
    assert message.startswith(f"Failed to parse {path} file (at path {path}).")
    assert message.endswith(reason)
    assert reason


@pytest.mark.asyncio
async def test_suffix_lookup_is_case_insensitive(base_dir: str):
    path = Path(base_dir) / "DATA.JSON"
    path.write_text(json.dumps(SIMPLE_RECORD), encoding="utf-8")
    assert await _records_from(path) == [SIMPLE_RECORD]


@pytest.mark.asyncio
async def test_sas7bdat_chunksize_keeps_column_types(sas7bdat_file: Path, monkeypatch):
    monkeypatch.setattr(Sas7bdatFileFormat, "chunksize", 2)
    records = await _records_from(sas7bdat_file)
    _assert_sas_records(records)


# Byte 70 of a sas7bdat header is the encoding code. 62 is cp1252, 20 is UTF-8.
SAS_ENCODING_OFFSET = 70
SAS_CP1252_ENCODING = 62
SAS_UTF8_ENCODING = 20


@pytest.mark.asyncio
async def test_sas7bdat_bad_text_byte_is_replaced(
    base_dir: str, sas7bdat_file: Path, caplog
):
    path = Path(base_dir) / "bad-byte.sas7bdat"
    data = bytearray(sas7bdat_file.read_bytes())
    assert data[SAS_ENCODING_OFFSET] == SAS_CP1252_ENCODING
    data[SAS_ENCODING_OFFSET] = SAS_UTF8_ENCODING
    pear = data.find(b"pear")
    assert pear != -1
    data[pear + 1] = 0xFF
    path.write_bytes(data)
    with caplog.at_level(
        logging.WARNING, logger="nodestream.pipeline.extractors.files"
    ):
        records = await _records_from(path)
    assert len(records) == 10
    assert records[0]["Column2"] == "p\ufffdar"
    assert records[1]["Column2"] == "dog"
    assert records[0]["Column3"] == 84.0
    assert isinstance(records[0]["Column3"], float)
    assert records[0]["Column4"] == datetime(1965, 12, 10)
    assert records[0]["Column8"] is None
    assert caplog.messages == [
        f"replaced undecodable bytes in 1 cells (columns: Column2) in {path}"
    ]


@pytest.mark.asyncio
async def test_replaced_sas_bytes_are_logged_once_per_file(
    base_dir: str, sas7bdat_file: Path, monkeypatch, caplog
):
    monkeypatch.setattr(Sas7bdatFileFormat, "chunksize", 1)
    path = Path(base_dir) / "bad-bytes.sas7bdat"
    data = bytearray(sas7bdat_file.read_bytes())
    data[SAS_ENCODING_OFFSET] = SAS_UTF8_ENCODING
    # Row 0 and row 1 of Column2, so the bad cells fall in different chunks.
    row0 = data.find(b"pear     ")
    row1 = data.find(b"dog      pear     apple    pear ")
    assert row0 != -1 and row1 != -1 and row0 != row1
    data[row0 + 1] = 0xFF
    data[row1 + 1] = 0xFF
    path.write_bytes(data)
    with caplog.at_level(
        logging.WARNING, logger="nodestream.pipeline.extractors.files"
    ):
        records = await _records_from(path)
    assert records[0]["Column2"] == "p\ufffdar"
    assert records[1]["Column2"] == "d\ufffdg"
    assert caplog.messages == [
        f"replaced undecodable bytes in 2 cells (columns: Column2) in {path}"
    ]


@pytest.mark.asyncio
async def test_unknown_sas_encoding_names_the_file(
    base_dir: str, sas7bdat_file: Path, caplog
):
    path = Path(base_dir) / "unknown-encoding.sas7bdat"
    data = bytearray(sas7bdat_file.read_bytes())
    data[SAS_ENCODING_OFFSET] = 0
    path.write_bytes(data)
    with caplog.at_level(
        logging.WARNING, logger="nodestream.pipeline.extractors.files"
    ):
        records = await _records_from(path)
    assert len(records) == 10
    assert records[0]["Column2"] == "pear"
    assert caplog.messages == [
        f"Could not infer SAS text encoding; decoding text as latin-1 in {path}."
    ]


def test_normalize_frame_keeps_numbers_as_floats():
    frame = pd.DataFrame(
        {
            "whole": [2.0, None, 4.0],
            "mixed": [2.0, 2.5, None],
            "when": [pd.Timestamp("2020-01-15"), pd.NaT, pd.Timestamp("2020-02-01")],
        }
    )
    records = _normalize_frame(frame)
    assert isinstance(records[0]["whole"], float)
    assert records[0]["whole"] == 2.0
    assert records[1]["whole"] is None
    assert records[2]["whole"] == 4.0
    assert isinstance(records[0]["mixed"], float)
    assert records[0]["mixed"] == 2.0
    assert records[1]["mixed"] == 2.5
    assert records[2]["mixed"] is None
    assert records[0]["when"] == datetime(2020, 1, 15)
    assert records[1]["when"] is None


def test_normalize_frame_replaces_undecodable_text():
    frame = pd.DataFrame({"name": [b"p\xffar", None, b"dog"]})
    records = _normalize_frame(frame, encoding="utf-8")
    assert records[0]["name"] == "p\ufffdar"
    assert records[1]["name"] is None
    assert records[2]["name"] == "dog"


def test_normalize_frame_decodes_non_ascii_column_names():
    def as_pandas_header(name: str) -> str:
        return name.encode("utf-8").decode("latin-1")

    groesse = as_pandas_header("Größe")
    citta = as_pandas_header("Città")
    angstrom = as_pandas_header("Å")
    padded = as_pandas_header("Grösse ")
    assert groesse == "Gr\u00c3\u00b6\u00c3\u009fe"
    assert citta.endswith("\u00a0")
    assert angstrom.endswith("\u0085")
    frame = pd.DataFrame(
        {
            groesse: [2.0],
            citta: ["rome"],
            angstrom: [1.0],
            padded: [3.0],
            "id": [1.0],
        }
    )
    records = _normalize_frame(frame, encoding="utf-8")
    assert list(records[0]) == ["Größe", "Città", "Å", "Grösse", "id"]
    assert records[0]["Größe"] == 2.0
    assert isinstance(records[0]["Größe"], float)
    assert records[0]["Città"] == "rome"
    assert records[0]["Å"] == 1.0
    assert isinstance(records[0]["Å"], float)
    assert records[0]["Grösse"] == 3.0
    assert records[0]["id"] == 1.0
    assert isinstance(records[0]["id"], float)


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("tables/one.sas7bdat", True),
        ("tables\\one.sas7bdat", True),
        ("/tables/one.sas7bdat", True),
        ("one.SAS7BDAT", True),
        ("notes/readme.txt", True),
        ("folder/", False),
        ("", False),
        ("__MACOSX/._one.sas7bdat", False),
        ("tables/._one.sas7bdat", False),
    ],
)
def test_is_data_member(name, expected):
    assert _is_data_member(name) is expected


def test_zip_is_an_archive_codec_not_a_file_format():
    assert ".zip" in SUPPORTED_ARCHIVE_FORMAT_REGISTRY
    assert ".zip" not in SUPPORTED_FILE_FORMAT_REGISTRY

    class PluginZip(FileCodec, alias=".zip"):
        def read_file_from_handle(self, reader):
            return []

    try:
        assert SUPPORTED_FILE_FORMAT_REGISTRY.get(".zip") is PluginZip
    finally:
        del SUPPORTED_FILE_FORMAT_REGISTRY.registry[".zip"]
