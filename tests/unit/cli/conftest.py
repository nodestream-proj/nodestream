import tempfile
from pathlib import Path

import pytest

from nodestream.project import PipelineDefinition, PipelineScope, Project


@pytest.fixture
def pipeline_definition(project_dir):
    return PipelineDefinition("dummy", project_dir / "dummy.yaml")


@pytest.fixture
def project_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def default_scope(pipeline_definition):
    return PipelineScope("default", [pipeline_definition])


@pytest.fixture
def project_with_default_scope(default_scope):
    return Project([default_scope], [])
