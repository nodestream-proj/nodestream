import pytest
from hamcrest import assert_that, equal_to

from nodestream.pipeline.extractors.apis import SimpleApiExtractor

TODO_API = "https://jsonplaceholder.typicode.com/todos"
TODOS_RESPONSE = [
    {"id": 1, "title": "delectus aut autem", "completed": False},
    {"id": 2, "title": "quis ut nam facilis et officia qui", "completed": True},
]


@pytest.mark.asyncio
async def test_api_no_pagination_no_nesting(httpx_mock):
    httpx_mock.add_response(url=TODO_API, json=TODOS_RESPONSE)
    subject = SimpleApiExtractor(TODO_API)
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to(TODOS_RESPONSE))


@pytest.mark.asyncio
async def test_api_no_pagination_with_nesting(httpx_mock):
    httpx_mock.add_response(url=TODO_API, json={"todos": TODOS_RESPONSE})
    subject = SimpleApiExtractor(TODO_API, yield_from="todos")
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to(TODOS_RESPONSE))


@pytest.mark.asyncio
async def test_api_pagination_no_nesting(httpx_mock):
    httpx_mock.add_response(url=TODO_API, json=TODOS_RESPONSE[:1])
    httpx_mock.add_response(url=TODO_API + "?o=1", json=TODOS_RESPONSE[1:])
    httpx_mock.add_response(url=TODO_API + "?o=2", json=[])
    subject = SimpleApiExtractor(TODO_API, offset_query_param="o")
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to(TODOS_RESPONSE))


@pytest.mark.asyncio
async def test_api_pagination_with_nesting(httpx_mock):
    httpx_mock.add_response(url=TODO_API, json={"todos": TODOS_RESPONSE[:1]})
    httpx_mock.add_response(url=TODO_API + "?o=1", json={"todos": TODOS_RESPONSE[1:]})
    httpx_mock.add_response(url=TODO_API + "?o=2", json={"todos": []})
    subject = SimpleApiExtractor(TODO_API, offset_query_param="o", yield_from="todos")
    results = [r async for r in subject.extract_records()]
    assert_that(results, equal_to(TODOS_RESPONSE))
