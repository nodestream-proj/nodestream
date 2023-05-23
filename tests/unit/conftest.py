import asyncio

import pytest

from nodestream.model import DesiredIngestion, InterpreterContext

DECENT_DOCUMENT = {
    "team": {
        "name": "nodestream",
    },
    "members": [
        {"first_name": "Zach", "last_name": "Probst"},
        {"first_name": "Chad", "last_name": "Cloes"},
    ],
    "project": {"tags": ["graphdb", "python"]},
}


@pytest.fixture
def blank_context():
    return InterpreterContext({}, DesiredIngestion())


@pytest.fixture
def blank_context_with_document():
    return InterpreterContext(DECENT_DOCUMENT, DesiredIngestion())


@pytest.fixture
def async_return():
    def _async_return(value=None):
        future = asyncio.Future()
        future.set_result(4)
        return future

    return _async_return
