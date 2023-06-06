import pytest
from hamcrest import assert_that, equal_to

from nodestream.cli.operations import GeneratePipelineScaffold


@pytest.mark.asyncio
async def test_generate_python_scaffold(project_dir):
    op = GeneratePipelineScaffold(project_dir, "neo4j")
    await op.perform(None)
    assert_that((project_dir / "pipelines" / "sample.yaml").exists(), equal_to(True))
