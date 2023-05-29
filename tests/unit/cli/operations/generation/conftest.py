import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def project_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)
