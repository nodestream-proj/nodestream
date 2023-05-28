import pytest

from nodestream.cli.operations.generation.generate_pipeline_scaffold import (
    GeneratePipelineScaffold,
)


@pytest.mark.asyncio
async def test_generate_python_scaffold(project_dir):
    op = GeneratePipelineScaffold(project_dir, "neo4j")
    await op.perform(None)
    assert (project_dir / "pipelines" / "sample.yaml").exists
