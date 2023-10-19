from typing import Dict, Optional

from httpx import AsyncClient

from .extractor import Extractor


class SimpleApiExtractor(Extractor):
    def __init__(
        self,
        url: str,
        yield_from: Optional[str] = None,
        offset_query_param: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        self.url = url
        self.offset_query_param = offset_query_param
        self.headers = headers
        self.yield_from = yield_from

    @property
    def does_pagination(self) -> bool:
        return self.offset_query_param is not None

    async def extract_records(self):
        should_continue = True
        records_so_far = 0
        params = None

        async with AsyncClient() as client:
            while should_continue:
                result = await client.get(self.url, headers=self.headers, params=params)
                result.raise_for_status()
                records = result.json()
                if self.yield_from:
                    records = records[self.yield_from]

                for item in records:
                    yield item

                should_continue = len(records) > 0 and self.does_pagination
                records_so_far += len(records)
                params = {self.offset_query_param: records_so_far}
