import json
from unittest.mock import AsyncMock, patch

import pytest
from hamcrest import assert_that, equal_to, has_entries, has_length
from httpx import HTTPStatusError

from nodestream.pipeline.extractors.stores.splunk_extractor import SplunkExtractor


@pytest.fixture
def mock_job_creation_response():
    """Mock response for job creation."""
    return '<?xml version="1.0" encoding="UTF-8"?><response><sid>1234567890.123</sid></response>'


@pytest.fixture
def mock_job_results():
    """Sample Splunk search results in the format returned by the results API."""
    return {
        "results": [
            {
                "_time": "2023-01-01T10:00:00",
                "host": "server1",
                "message": "Login successful",
            },
            {
                "_time": "2023-01-01T10:01:00",
                "host": "server2",
                "message": "Error occurred",
            },
            {"_time": "2023-01-01T10:02:00", "host": "server1", "message": "Logout"},
        ]
    }


@pytest.fixture
def splunk_extractor():
    """Create a SplunkExtractor instance for testing."""
    return SplunkExtractor.from_file_data(
        base_url="https://splunk.example.com:8089",
        query="index=test | head 10",
        auth_token="test-token-123",
        earliest_time="-1h",
        latest_time="now",
        chunk_size=100,
    )


@pytest.fixture
def splunk_extractor_basic_auth():
    """Create a SplunkExtractor instance with basic auth for testing."""
    return SplunkExtractor.from_file_data(
        base_url="https://splunk.example.com:8089",
        query="index=test",
        username="testuser",
        password="testpass",
    )


class TestSplunkExtractorInitialization:
    def test_from_file_data_with_token_auth(self):
        extractor = SplunkExtractor.from_file_data(
            base_url="https://splunk.example.com:8089/",  # trailing slash should be stripped
            query="index=main",
            auth_token="my-token",
            earliest_time="-24h",
            chunk_size=500,
        )

        assert_that(extractor.base_url, equal_to("https://splunk.example.com:8089"))
        assert_that(extractor.query, equal_to("index=main"))
        assert_that(extractor.auth_token, equal_to("my-token"))
        assert_that(extractor.username, equal_to(None))
        assert_that(extractor.password, equal_to(None))
        assert_that(extractor.earliest_time, equal_to("-24h"))
        assert_that(extractor.chunk_size, equal_to(500))

    def test_from_file_data_with_basic_auth(self):
        extractor = SplunkExtractor.from_file_data(
            base_url="https://splunk.example.com:8089",
            query="index=security",
            username="admin",
            password="secret",
        )

        assert_that(extractor.auth_token, equal_to(None))
        assert_that(extractor.username, equal_to("admin"))
        assert_that(extractor.password, equal_to("secret"))

    def test_from_file_data_defaults(self):
        extractor = SplunkExtractor.from_file_data(
            base_url="https://splunk.example.com:8089",
            query="index=main",
        )

        assert_that(extractor.verify_ssl, equal_to(True))
        assert_that(extractor.request_timeout_seconds, equal_to(300))
        assert_that(extractor.chunk_size, equal_to(1000))


class TestSplunkExtractorHelpers:
    def test_auth_property_with_token(self, splunk_extractor):
        assert_that(splunk_extractor._auth, equal_to(None))  # Token goes in header

    def test_auth_property_with_basic_auth(self, splunk_extractor_basic_auth):
        auth = splunk_extractor_basic_auth._auth
        assert_that(auth is not None, equal_to(True))

    def test_headers_with_token(self, splunk_extractor):
        headers = splunk_extractor._headers
        assert_that(
            headers,
            has_entries(
                {"Accept": "application/json", "Authorization": "Splunk test-token-123"}
            ),
        )

    def test_headers_without_token(self, splunk_extractor_basic_auth):
        headers = splunk_extractor_basic_auth._headers
        assert_that(headers, equal_to({"Accept": "application/json"}))

    def test_normalized_query_adds_search_prefix(self):
        # Query already has search prefix
        extractor = SplunkExtractor.from_file_data(
            base_url="https://splunk.example.com:8089",
            query="search index=main",
        )
        assert_that(extractor._normalized_query, equal_to("search index=main"))

        # Query without search prefix
        extractor2 = SplunkExtractor.from_file_data(
            base_url="https://splunk.example.com:8089",
            query="index=main | head 10",
        )
        assert_that(
            extractor2._normalized_query, equal_to("search index=main | head 10")
        )

    def test_endpoint_urls(self, splunk_extractor):
        jobs_endpoint = splunk_extractor.get_jobs_endpoint()
        assert_that(
            jobs_endpoint,
            equal_to(
                "https://splunk.example.com:8089/servicesNS/admin/search/search/jobs"
            ),
        )

        results_endpoint = splunk_extractor.get_results_endpoint("test123")
        assert_that(
            results_endpoint,
            equal_to(
                "https://splunk.example.com:8089/servicesNS/admin/search/search/jobs/test123/results"
            ),
        )


class TestSplunkExtractorJobManagement:
    @pytest.mark.asyncio
    async def test_create_search_job_success(
        self, splunk_extractor, mock_job_creation_response, mocker
    ):
        mock_response = mocker.MagicMock()
        mock_response.status_code = 201
        mock_response.text = mock_job_creation_response
        mock_response.json.side_effect = json.JSONDecodeError("Not JSON", "", 0)

        mock_client = mocker.MagicMock()
        mock_client.post = AsyncMock(return_value=mock_response)

        search_id = await splunk_extractor._create_search_job(mock_client)

        assert_that(search_id, equal_to("1234567890.123"))
        mock_client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_search_job_json_response(self, splunk_extractor, mocker):
        """Test job creation with JSON response (preferred format)."""
        mock_response = mocker.MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"sid": "json123"}

        mock_client = mocker.MagicMock()
        mock_client.post = AsyncMock(return_value=mock_response)

        search_id = await splunk_extractor._create_search_job(mock_client)

        assert_that(search_id, equal_to("json123"))
        # Verify we requested JSON format
        call_args = mock_client.post.call_args
        assert_that(call_args[1]["data"]["output_mode"], equal_to("json"))

    @pytest.mark.asyncio
    async def test_create_search_job_xml_fallback(
        self, splunk_extractor, mock_job_creation_response, mocker
    ):
        """Test job creation with XML response (fallback when JSON fails)."""
        mock_response = mocker.MagicMock()
        mock_response.status_code = 201
        mock_response.text = mock_job_creation_response
        mock_response.json.side_effect = json.JSONDecodeError("Not JSON", "", 0)

        mock_client = mocker.MagicMock()
        mock_client.post = AsyncMock(return_value=mock_response)

        search_id = await splunk_extractor._create_search_job(mock_client)

        assert_that(search_id, equal_to("1234567890.123"))
        mock_client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_search_job_malformed_xml(self, splunk_extractor, mocker):
        """Test job creation with malformed XML response."""
        mock_response = mocker.MagicMock()
        mock_response.status_code = 201
        mock_response.text = "Invalid XML <unclosed"
        mock_response.json.side_effect = json.JSONDecodeError("Not JSON", "", 0)

        mock_client = mocker.MagicMock()
        mock_client.post = AsyncMock(return_value=mock_response)

        with pytest.raises(RuntimeError, match="Failed to extract search ID"):
            await splunk_extractor._create_search_job(mock_client)

    @pytest.mark.asyncio
    async def test_wait_for_job_completion_success(self, splunk_extractor, mocker):
        # Mock job status responses - first RUNNING, then DONE
        mock_response_running = mocker.MagicMock()
        mock_response_running.status_code = 200
        mock_response_running.json.return_value = {
            "entry": [{"content": {"dispatchState": "RUNNING"}}]
        }

        mock_response_done = mocker.MagicMock()
        mock_response_done.status_code = 200
        mock_response_done.json.return_value = {
            "entry": [{"content": {"dispatchState": "DONE"}}]
        }

        mock_client = mocker.MagicMock()
        mock_client.get = AsyncMock(
            side_effect=[mock_response_running, mock_response_done]
        )

        # Should complete without raising
        await splunk_extractor._wait_for_job_completion(
            mock_client, "test123", max_wait_seconds=10
        )

    @pytest.mark.asyncio
    async def test_wait_for_job_completion_json_response(
        self, splunk_extractor, mocker
    ):
        """Test job status checking with JSON response (expected format)."""
        mock_response_running = mocker.MagicMock()
        mock_response_running.status_code = 200
        mock_response_running.json.return_value = {
            "entry": [{"content": {"dispatchState": "RUNNING"}}]
        }

        mock_response_done = mocker.MagicMock()
        mock_response_done.status_code = 200
        mock_response_done.json.return_value = {
            "entry": [{"content": {"dispatchState": "DONE"}}]
        }

        mock_client = mocker.MagicMock()
        mock_client.get = AsyncMock(
            side_effect=[mock_response_running, mock_response_done]
        )

        await splunk_extractor._wait_for_job_completion(
            mock_client, "test123", max_wait_seconds=10
        )

        # Verify we requested JSON format
        call_args = mock_client.get.call_args_list[0]
        assert_that(call_args[1]["params"]["output_mode"], equal_to("json"))

    @pytest.mark.asyncio
    async def test_wait_for_job_completion_xml_fallback(self, splunk_extractor, mocker):
        """Test job status checking with XML response (fallback)."""
        xml_response = """<?xml version="1.0" encoding="UTF-8"?>
        <response>
            <entry>
                <content>
                    <key name="dispatchState">DONE</key>
                </content>
            </entry>
        </response>"""

        mock_response = mocker.MagicMock()
        mock_response.status_code = 200
        mock_response.text = xml_response
        mock_response.json.side_effect = json.JSONDecodeError("Not JSON", "", 0)

        mock_client = mocker.MagicMock()
        mock_client.get = AsyncMock(return_value=mock_response)

        # Should complete without raising (finds DONE in XML)
        await splunk_extractor._wait_for_job_completion(
            mock_client, "test123", max_wait_seconds=10
        )

    @pytest.mark.asyncio
    async def test_wait_for_job_completion_failure(self, splunk_extractor, mocker):
        mock_response = mocker.MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "entry": [{"content": {"dispatchState": "FAILED"}}]
        }

        mock_client = mocker.MagicMock()
        mock_client.get = AsyncMock(return_value=mock_response)

        with pytest.raises(RuntimeError, match="Search job failed"):
            await splunk_extractor._wait_for_job_completion(mock_client, "test123")


class TestSplunkExtractorResultsRetrieval:
    @pytest.mark.asyncio
    async def test_get_job_results_single_chunk(
        self, splunk_extractor, mock_job_results, mocker
    ):
        mock_response = mocker.MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_job_results

        mock_client = mocker.MagicMock()
        mock_client.get = AsyncMock(return_value=mock_response)

        results = []
        async for result in splunk_extractor._get_job_results(mock_client, "test123"):
            results.append(result)

        assert_that(results, has_length(3))
        assert_that(
            results[0],
            has_entries(
                {
                    "_time": "2023-01-01T10:00:00",
                    "host": "server1",
                    "message": "Login successful",
                }
            ),
        )

    @pytest.mark.asyncio
    async def test_get_job_results_json_parse_error(self, splunk_extractor, mocker):
        """Test results retrieval when JSON parsing fails."""
        mock_response = mocker.MagicMock()
        mock_response.status_code = 200
        mock_response.text = "Invalid JSON response"
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)

        mock_client = mocker.MagicMock()
        mock_client.get = AsyncMock(return_value=mock_response)

        results = []
        async for result in splunk_extractor._get_job_results(mock_client, "test123"):
            results.append(result)

        # Should handle gracefully and return empty results
        assert_that(results, has_length(0))
        assert_that(splunk_extractor.is_done, equal_to(True))

    @pytest.mark.asyncio
    async def test_get_job_results_pagination(self, splunk_extractor, mocker):
        # Set small chunk size for testing
        splunk_extractor.chunk_size = 2

        # Mock responses for pagination
        first_response = mocker.MagicMock()
        first_response.status_code = 200
        first_response.json.return_value = {
            "results": [
                {"_time": "2023-01-01T10:00:00", "host": "server1"},
                {"_time": "2023-01-01T10:01:00", "host": "server2"},
            ]
        }

        second_response = mocker.MagicMock()
        second_response.status_code = 200
        second_response.json.return_value = {
            "results": [
                {"_time": "2023-01-01T10:02:00", "host": "server3"},
            ]
        }

        mock_client = mocker.MagicMock()
        mock_client.get = AsyncMock(side_effect=[first_response, second_response])

        results = []
        async for result in splunk_extractor._get_job_results(mock_client, "test123"):
            results.append(result)

        assert_that(results, has_length(3))
        assert_that(splunk_extractor.offset, equal_to(3))
        assert_that(splunk_extractor.is_done, equal_to(True))


class TestSplunkExtractorExtraction:
    @pytest.mark.asyncio
    async def test_extract_records_full_flow(self, splunk_extractor, mocker):
        with patch(
            "nodestream.pipeline.extractors.stores.splunk_extractor.AsyncClient"
        ) as mock_client_class:
            mock_client = mocker.MagicMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client
            mock_client_class.return_value.__aexit__.return_value = None

            # Mock job creation
            job_response = mocker.MagicMock()
            job_response.status_code = 201
            job_response.json.return_value = {"sid": "test123"}
            mock_client.post = AsyncMock(return_value=job_response)

            # Mock job completion
            status_response = mocker.MagicMock()
            status_response.status_code = 200
            status_response.json.return_value = {
                "entry": [{"content": {"dispatchState": "DONE"}}]
            }

            # Mock results
            results_response = mocker.MagicMock()
            results_response.status_code = 200
            results_response.json.return_value = {
                "results": [
                    {"_time": "2023-01-01T10:00:00", "host": "server1"},
                ]
            }

            # Setup get calls: first for status check, then for results
            mock_client.get = AsyncMock(side_effect=[status_response, results_response])

            records = []
            async for record in splunk_extractor.extract_records():
                records.append(record)

            assert_that(records, has_length(1))
            assert_that(
                records[0],
                has_entries({"_time": "2023-01-01T10:00:00", "host": "server1"}),
            )


class TestSplunkExtractorCheckpointing:
    @pytest.mark.asyncio
    async def test_make_checkpoint(self, splunk_extractor):
        splunk_extractor.search_id = "test123"
        splunk_extractor.offset = 100
        splunk_extractor.is_done = False

        checkpoint = await splunk_extractor.make_checkpoint()

        assert_that(
            checkpoint,
            equal_to({"search_id": "test123", "offset": 100, "is_done": False}),
        )

    @pytest.mark.asyncio
    async def test_resume_from_checkpoint(self, splunk_extractor):
        checkpoint = {"search_id": "restored123", "offset": 50, "is_done": False}

        await splunk_extractor.resume_from_checkpoint(checkpoint)

        assert_that(splunk_extractor.search_id, equal_to("restored123"))
        assert_that(splunk_extractor.offset, equal_to(50))
        assert_that(splunk_extractor.is_done, equal_to(False))
