import pytest
from hamcrest import assert_that

from nodestream.cli.operations import GeneratePythonScaffold

from tests.unit.matchers import exists


@pytest.mark.asyncio
async def test_generate_python_scaffold(project_dir):
    op = GeneratePythonScaffold(project_dir)
    await op.perform(None)
    assert_that(project_dir / project_dir.stem / "__init__.py", exists())
    assert_that(project_dir / project_dir.stem / "argument_resolvers.py", exists())
    assert_that(project_dir / project_dir.stem / "normalizers.py", exists())
    assert_that(project_dir / project_dir.stem / "value_providers.py", exists())
