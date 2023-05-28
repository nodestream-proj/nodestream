import pytest

from nodestream.cli.operations.generation.generate_python_scaffold import (
    GeneratePythonScaffold,
)


@pytest.mark.asyncio
async def test_generate_python_scaffold(project_dir):
    op = GeneratePythonScaffold(project_dir)
    await op.perform(None)
    assert (project_dir / project_dir.stem / "__init__.py").exists
    assert (project_dir / project_dir.stem / "argument_resolvers.py").exists
    assert (project_dir / project_dir.stem / "normalizers.py").exists
    assert (project_dir / project_dir.stem / "value_providers.py").exists
