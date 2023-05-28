import pytest

import tempfile
from pathlib import Path


@pytest.fixture
def project_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)
