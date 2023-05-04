import pytest

from nodestream.model import DesiredIngestion, InterpreterContext


@pytest.fixture
def blank_context():
    return InterpreterContext({}, DesiredIngestion())
