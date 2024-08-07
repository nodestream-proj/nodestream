from contextlib import asynccontextmanager
from io import BytesIO, IOBase
from logging import getLogger
from pathlib import Path
from typing import Iterable, Optional

from ...credential_utils import AwsClientFactory
from ...files import FileSource, ReadableFile, UnifiedFileExtractor

LOGGER = getLogger(__name__)


class S3File(ReadableFile):
    def __init__(
        self,
        key: str,
        s3_client,
        bucket: str,
        archive_dir: str | None,
        object_format: str | None,
    ) -> None:
        self.key = key
        self.s3_client = s3_client
        self.bucket = bucket
        self.archive_dir = archive_dir
        self.object_format = object_format

    def archive_if_required(self, key: str):
        if not self.archive_dir:
            return

        LOGGER.info("Archiving S3 Object", extra=dict(key=key))
        filename = Path(key).name
        self.s3_client.copy(
            Bucket=self.bucket,
            Key=f"{self.archive_dir}/{filename}",
            CopySource={"Bucket": self.bucket, "Key": key},
        )
        self.s3_client.delete_object(Bucket=self.bucket, Key=key)

    def path_like(self) -> Path:
        path = Path(self.key)
        return path.with_suffix(self.object_format or path.suffix)

    @asynccontextmanager
    async def as_reader(self, reader: IOBase):
        streaming_body = self.s3_client.get_object(Bucket=self.bucket, Key=self.key)[
            "Body"
        ]
        yield reader(BytesIO(streaming_body.read()))
        self.archive_if_required(self.key)


class S3FileSource(FileSource, alias="s3"):
    @classmethod
    def from_file_data(
        cls,
        bucket: str,
        prefix: Optional[str] = None,
        archive_dir: Optional[str] = None,
        object_format: Optional[str] = None,
        **aws_client_args,
    ):
        return cls(
            bucket=bucket,
            prefix=prefix,
            archive_dir=archive_dir,
            object_format=object_format,
            s3_client=AwsClientFactory(**aws_client_args).make_client("s3"),
        )

    def __init__(
        self,
        bucket: str,
        s3_client,
        archive_dir: Optional[str] = None,
        object_format: Optional[str] = None,
        prefix: Optional[str] = None,
    ):
        self.bucket = bucket
        self.s3_client = s3_client
        self.archive_dir = archive_dir
        self.object_format = object_format
        self.prefix = prefix or ""

    def object_is_in_archive(self, key: str) -> bool:
        return key.startswith(self.archive_dir) if self.archive_dir else False

    def find_keys_in_bucket(self) -> Iterable[str]:
        # Returns all keys in the bucket that are not in the archive dir
        # and have the prefix.
        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)
        for page in page_iterator:
            keys = (obj["Key"] for obj in page.get("Contents", []))
            yield from filter(lambda k: not self.object_is_in_archive(k), keys)

    async def get_files(self):
        for key in self.find_keys_in_bucket():
            yield S3File(
                key=key,
                s3_client=self.s3_client,
                bucket=self.bucket,
                archive_dir=self.archive_dir,
                object_format=self.object_format,
            )


class S3Extractor(UnifiedFileExtractor):
    """Extracts files from S3.


    NOTE: This class is slated for deprecation in favor of the more general
    `UnifiedFileExtractor` class. It is recommended to use the
    `UnifiedFileExtractor` class directly instead of this class.
    """

    @classmethod
    def from_file_data(cls, **kwargs):
        return cls([S3FileSource.from_file_data(**kwargs)])
