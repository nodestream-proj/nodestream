from contextlib import contextmanager
from logging import getLogger
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, AsyncGenerator, Optional

from ...credential_utils import AwsClientFactory
from ...extractor import Extractor
from ...files import SupportedFileFormat


class S3Extractor(Extractor):
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
            object_format=object_format,
            prefix=prefix,
            archive_dir=archive_dir,
            s3_client=AwsClientFactory(**aws_client_args).make_client("s3"),
        )

    def __init__(
        self,
        bucket: str,
        s3_client,
        archive_dir: Optional[str] = None,
        object_format: Optional[str] = None,
        prefix: Optional[str] = None,
    ) -> None:
        self.object_format = object_format
        self.prefix = prefix or ""
        self.bucket = bucket
        self.archive_dir = archive_dir
        self.s3_client = s3_client
        self.logger = getLogger(__name__)

    @contextmanager
    def get_object_as_tempfile(self, key: str):
        streaming_body = self.s3_client.get_object(Bucket=self.bucket, Key=key)["Body"]
        suffixes = "".join(Path(key).suffixes)
        temp_file = NamedTemporaryFile("w+b", suffix=suffixes)
        for chunk in iter(lambda: streaming_body.read(1024), b""):
            temp_file.write(chunk)
        temp_file.flush()
        yield temp_file

    def archive_s3_object(self, key: str):
        if self.archive_dir:
            self.logger.info("Archiving S3 Object", extra=dict(key=key))
            filename = Path(key).name
            self.s3_client.copy(
                Bucket=self.bucket,
                Key=f"{self.archive_dir}/{filename}",
                CopySource={"Bucket": self.bucket, "Key": key},
            )
            self.s3_client.delete_object(Bucket=self.bucket, Key=key)

    def infer_object_format(self, key: str) -> str:
        object_format = self.object_format or Path(key).suffix
        if not object_format:
            raise ValueError(
                f"No object format provided and key has no extension: '{key}'"
            )
        return object_format

    @contextmanager
    def get_object_as_file(self, key: str) -> SupportedFileFormat:
        with self.get_object_as_tempfile(key) as temp_file:
            path = Path(temp_file.name)
            with SupportedFileFormat.open(path) as file_format:
                yield file_format

    def is_object_in_archive(self, key: str) -> bool:
        if self.archive_dir:
            return key.startswith(self.archive_dir)
        return False

    def find_keys_in_bucket(self) -> list[str]:
        # Returns all keys in the bucket that are not in the archive dir and have the prefix.
        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)
        for page in page_iterator:
            keys = (obj["Key"] for obj in page.get("Contents", []))
            yield from filter(lambda k: not self.is_object_in_archive(k), keys)

    async def extract_records(self) -> AsyncGenerator[Any, Any]:
        for key in self.find_keys_in_bucket():
            try:
                with self.get_object_as_file(key) as records:
                    for record in records.read_file():
                        yield record
            finally:
                self.archive_s3_object(key)
