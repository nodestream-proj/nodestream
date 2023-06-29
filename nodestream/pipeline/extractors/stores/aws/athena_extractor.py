import time
from decimal import Decimal
from logging import getLogger
from typing import Any, AsyncGenerator

from ...extractor import Extractor
from .credential_utils import AwsClientFactory

ATHENA_STATE_RUNNING = "RUNNING"
ATHENA_STATE_FAILED = "FAILED"
ATHENA_STATE_CANCELLED = "CANCELLED"
ATHENA_STATE_QUEUED = "QUEUED"

PENDING_ATHENA_STATES = {ATHENA_STATE_QUEUED, ATHENA_STATE_RUNNING}
BAD_ATEHNA_STATES = {ATHENA_STATE_FAILED, ATHENA_STATE_CANCELLED}


CONVERTERS = {
    "tinyint": int,
    "smallint": int,
    "integer": int,
    "bigint": int,
    "double": float,
    "float": float,
    "decimal": Decimal,
    "char": str,
    "string": str,
    "boolean": lambda v: v == "true",
}


class AthenaRowConverter:
    def __init__(self, column_meta) -> None:
        self.column_meta = column_meta

    def convert_row(self, row):
        return {
            column_meta["Name"]: self.convert_value(column_meta, value)
            for column_meta, value in zip(self.column_meta, row["Data"])
        }

    def convert_value(self, column_metadata, column_value):
        raw_value = column_value.get("VarCharValue")
        if raw_value is None:
            return None
        return CONVERTERS[column_metadata["Type"]](raw_value)


class AthenaExtractor(Extractor):
    @classmethod
    def from_file_data(
        cls,
        query: str,
        database: str,
        workgroup: str,
        output_location: str,
        poll_interval_seconds: int = 1,
        page_size: int = 500,
        **aws_client_args,
    ):
        client = AwsClientFactory(**aws_client_args).make_client("athena")
        return cls(
            query=query,
            database=database,
            workgroup=workgroup,
            output_location=output_location,
            poll_interval_seconds=poll_interval_seconds,
            page_size=page_size,
            client=client,
        )

    def __init__(
        self,
        query: str,
        database: str,
        workgroup: str,
        output_location: str,
        client,
        poll_interval_seconds: int,
        page_size: int,
    ) -> None:
        self.query = query
        self.database = database
        self.workgroup = workgroup
        self.output_location = output_location
        self.client = client
        self.poll_interval_seconds = poll_interval_seconds
        self.page_size = page_size
        self.logger = getLogger(self.__class__.__name__)

    def execute_query(self):
        result = self.client.start_query_execution(
            QueryString=self.query,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.output_location},
            WorkGroup=self.workgroup,
        )
        self.query_execution_id = result["QueryExecutionId"]
        self.logger.debug(
            "Athena Query Started", dict(query_execution_id=self.query_execution_id)
        )

    def get_query_status(self):
        result = self.client.get_query_execution(
            QueryExecutionId=self.query_execution_id
        )
        return result["QueryExecution"]["Status"]["State"]

    def await_query_completion(self):
        while (status := self.get_query_status()) in PENDING_ATHENA_STATES:
            time.sleep(self.poll_interval_seconds)

        if status in BAD_ATEHNA_STATES:
            raise RuntimeError(f"Failed with bad athena query state: {status}")

    def get_result_paginator(self):
        paginator = self.client.get_paginator("get_query_results")
        return paginator.paginate(
            QueryExecutionId=self.query_execution_id,
            PaginationConfig={"PageSize": self.page_size},
        )

    def page_results_and_get_rows_with_metadata(self):
        for page in self.get_result_paginator():
            column_meta = page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
            for row in page["ResultSet"]["Rows"]:
                yield row, column_meta

    def convert_data_types_of_rows_based_on_headers(self, rows_with_meta):
        row, page_meta = next(rows_with_meta)
        converter = AthenaRowConverter(page_meta)
        yield converter.convert_row(row)
        for row, _ in rows_with_meta:
            yield converter.convert_row(row)

    async def extract_records(self) -> AsyncGenerator[Any, Any]:
        self.execute_query()
        self.await_query_completion()
        rows_with_meta = self.page_results_and_get_rows_with_metadata()
        for result in self.convert_data_types_of_rows_based_on_headers(rows_with_meta):
            yield result
