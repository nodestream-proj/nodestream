import json
import asyncio
from logging import getLogger
from typing import Any, AsyncGenerator, Dict, Optional

from httpx import AsyncClient, BasicAuth, HTTPStatusError

from ..extractor import Extractor


class SplunkRecord:
    """
    Wrapper for Splunk search result records.
    """

    @classmethod
    def from_raw_splunk_result(cls, raw_result: Dict[str, Any]):
        """Create a SplunkRecord from raw Splunk API response."""
        return cls(raw_result)

    def __init__(self, record_data: Dict[str, Any]):
        self.record_data = record_data


class SplunkExtractor(Extractor):
    @classmethod
    def from_file_data(
        cls,
        base_url: str,
        query: str,
        auth_token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        earliest_time: str = "-24h",
        latest_time: str = "now",
        verify_ssl: bool = True,
        request_timeout_seconds: int = 300,
        max_count: int = 10000,
        app: str = "search",
        user: Optional[str] = None,
        chunk_size: int = 1000,
    ) -> "SplunkExtractor":
        return cls(
            base_url=base_url.rstrip("/"),
            query=query,
            auth_token=auth_token,
            username=username,
            password=password,
            earliest_time=earliest_time,
            latest_time=latest_time,
            verify_ssl=verify_ssl,
            request_timeout_seconds=request_timeout_seconds,
            max_count=max_count,
            app=app,
            user=user or username or "admin",
            chunk_size=chunk_size,
        )

    def __init__(
        self,
        base_url: str,
        query: str,
        auth_token: Optional[str],
        username: Optional[str],
        password: Optional[str],
        earliest_time: str,
        latest_time: str,
        verify_ssl: bool,
        request_timeout_seconds: int,
        max_count: int,
        app: str,
        user: str,
        chunk_size: int,
    ) -> None:
        self.base_url = base_url
        self.query = query
        self.auth_token = auth_token
        self.username = username
        self.password = password
        self.earliest_time = earliest_time
        self.latest_time = latest_time
        self.verify_ssl = verify_ssl
        self.request_timeout_seconds = request_timeout_seconds
        self.max_count = max_count
        self.app = app
        self.user = user
        self.chunk_size = chunk_size

        # State for pagination and checkpointing
        self.search_id = None
        self.offset = 0
        self.is_done = False

        self.logger = getLogger(self.__class__.__name__)

    @property
    def _auth(self):
        if self.auth_token:
            return None  # token goes in header, not in BasicAuth
        if self.username is not None and self.password is not None:
            return BasicAuth(self.username, self.password)
        return None

    @property
    def _headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.auth_token:
            headers["Authorization"] = f"Splunk {self.auth_token}"
        return headers

    @property
    def _normalized_query(self) -> str:
        """Ensure query starts with 'search' keyword."""
        query = self.query.strip()
        if not query.lower().startswith("search "):
            return f"search {query}"
        return query

    def get_jobs_endpoint(self) -> str:
        """Get the Splunk jobs endpoint."""
        print(self.base_url, self.user, self.app, "base_url, user, app")
        print(
            f"{self.base_url}/servicesNS/{self.user}/{self.app}/search/jobs",
            "jobs_endpoint",
        )
        return f"{self.base_url}/servicesNS/{self.user}/{self.app}/search/jobs"

    def get_results_endpoint(self, search_id: str) -> str:
        """Get the results endpoint for a specific search job."""
        return f"{self.base_url}/servicesNS/{self.user}/{self.app}/search/jobs/{search_id}/results"

    async def _create_search_job(self, client: AsyncClient) -> str:
        """Create a search job using the official Splunk REST API pattern."""
        job_data = {
            "search": self._normalized_query,
            "earliest_time": self.earliest_time,
            "latest_time": self.latest_time,
            "max_count": str(self.max_count),
            "output_mode": "json",  # Request JSON format explicitly
        }

        self.logger.info(
            "Creating Splunk search job",
            extra={
                "query": self._normalized_query,
                "earliest_time": self.earliest_time,
                "latest_time": self.latest_time,
                "max_count": self.max_count,
            },
        )

        response = await client.post(
            self.get_jobs_endpoint(),
            data=job_data,
            headers={
                **self._headers,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            auth=self._auth,
        )

        if response.status_code != 201:
            raise HTTPStatusError(
                f"Failed to create search job: {response.status_code}",
                request=response.request,
                response=response,
            )

        # Extract SID from response - try JSON first since we requested it
        search_id = None

        try:
            result = response.json()
            search_id = result.get("sid")
            if search_id:
                self.logger.info(
                    "Created search job successfully", extra={"search_id": search_id}
                )
                return search_id
        except json.JSONDecodeError:
            self.logger.debug("Job creation response not JSON, trying XML parsing")

        # Fallback to XML parsing
        import xml.etree.ElementTree as ET

        try:
            root = ET.fromstring(response.text)
            sid_elem = root.find(".//sid")
            if sid_elem is not None:
                search_id = sid_elem.text
        except Exception as e:
            self.logger.error(
                "Failed to parse job creation response",
                extra={"error": str(e), "response": response.text[:500]},
            )

        if not search_id:
            raise RuntimeError(
                f"Failed to extract search ID from response: {response.text[:500]}"
            )

        self.logger.info(
            "Created search job successfully", extra={"search_id": search_id}
        )
        return search_id

    async def _wait_for_job_completion(
        self, client: AsyncClient, search_id: str, max_wait_seconds: int = 300
    ):
        """Wait for a search job to complete."""
        wait_count = 0

        while wait_count < max_wait_seconds:
            response = await client.get(
                f"{self.get_jobs_endpoint()}/{search_id}",
                params={"output_mode": "json"},
                headers=self._headers,
                auth=self._auth,
            )

            if response.status_code != 200:
                raise HTTPStatusError(
                    f"Failed to check job status: {response.status_code}",
                    request=response.request,
                    response=response,
                )

            dispatch_state = "UNKNOWN"

            try:
                job_status = response.json()
                dispatch_state = (
                    job_status.get("entry", [{}])[0]
                    .get("content", {})
                    .get("dispatchState")
                )
            except (json.JSONDecodeError, KeyError, IndexError):
                # Try XML parsing
                import xml.etree.ElementTree as ET

                try:
                    root = ET.fromstring(response.text)
                    for elem in root.iter():
                        if (
                            "dispatchState" in elem.tag
                            or elem.get("name") == "dispatchState"
                        ):
                            dispatch_state = elem.text
                            break
                except Exception as e:
                    self.logger.warning(
                        "Failed to parse job status",
                        extra={"error": str(e), "search_id": search_id},
                    )

            self.logger.debug(
                "Job status check",
                extra={
                    "search_id": search_id,
                    "dispatch_state": dispatch_state,
                    "wait_time": wait_count,
                },
            )

            if dispatch_state == "DONE":
                self.logger.info("Search job completed", extra={"search_id": search_id})
                return
            elif dispatch_state == "FAILED":
                raise RuntimeError(f"Search job failed: {search_id}")

            await asyncio.sleep(2)  # Wait 2 seconds before checking again
            wait_count += 2

        raise RuntimeError(
            f"Search job timed out after {max_wait_seconds} seconds: {search_id}"
        )

    async def _get_job_results(
        self, client: AsyncClient, search_id: str
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Get results from a completed search job with pagination."""
        while not self.is_done:
            params = {
                "output_mode": "json",
                "count": str(self.chunk_size),
                "offset": str(self.offset),
            }

            response = await client.get(
                self.get_results_endpoint(search_id),
                params=params,
                headers=self._headers,
                auth=self._auth,
            )

            if response.status_code != 200:
                raise HTTPStatusError(
                    f"Failed to get job results: {response.status_code}",
                    request=response.request,
                    response=response,
                )

            try:
                results = response.json()
                results_list = results.get("results", [])
            except json.JSONDecodeError:
                self.logger.error(
                    "Failed to parse results as JSON",
                    extra={"search_id": search_id, "response": response.text[:500]},
                )
                results_list = []

            if not results_list:
                self.is_done = True
                break

            self.logger.debug(
                "Retrieved results chunk",
                extra={
                    "search_id": search_id,
                    "chunk_size": len(results_list),
                    "offset": self.offset,
                    "total_offset": self.offset + len(results_list),
                },
            )

            for result in results_list:
                yield result

            self.offset += len(results_list)

            if len(results_list) < self.chunk_size:
                self.is_done = True

    async def extract_records(self) -> AsyncGenerator[Any, Any]:
        """Extract records using the official Splunk REST API job-based approach."""
        async with AsyncClient(
            verify=self.verify_ssl, timeout=self.request_timeout_seconds
        ) as client:
            try:
                # Step 1: Create search job if we don't have one
                if not self.search_id:
                    self.search_id = await self._create_search_job(client)
                    await self._wait_for_job_completion(client, self.search_id)

                record_count = 0
                async for result in self._get_job_results(client, self.search_id):
                    record_count += 1
                    yield SplunkRecord.from_raw_splunk_result(result).record_data

                self.logger.info(
                    "Extraction completed",
                    extra={"search_id": self.search_id, "total_records": record_count},
                )

            except HTTPStatusError as exc:
                self.logger.error(
                    "Splunk request failed",
                    extra={
                        "status_code": exc.response.status_code,
                        "content": (
                            exc.response.text[:500]
                            if hasattr(exc.response, "text")
                            else str(exc.response)
                        ),
                        "url": str(exc.request.url),
                        "query": self._normalized_query,
                    },
                )
                raise
            except Exception as exc:
                self.logger.error(
                    "Unexpected error during extraction",
                    extra={
                        "error": str(exc),
                        "error_type": type(exc).__name__,
                        "query": self._normalized_query,
                        "search_id": self.search_id,
                    },
                )
                raise

    async def make_checkpoint(self):
        """Create a checkpoint for resuming extraction."""
        return {
            "search_id": self.search_id,
            "offset": self.offset,
            "is_done": self.is_done,
        }

    async def resume_from_checkpoint(self, checkpoint_object):
        """Resume extraction from a checkpoint."""
        if checkpoint_object:
            self.search_id = checkpoint_object.get("search_id")
            self.offset = checkpoint_object.get("offset", 0)
            self.is_done = checkpoint_object.get("is_done", False)

            self.logger.info(
                "Resuming from checkpoint",
                extra={
                    "search_id": self.search_id,
                    "offset": self.offset,
                    "is_done": self.is_done,
                },
            )
