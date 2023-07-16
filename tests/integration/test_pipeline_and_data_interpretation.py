import json
from dataclasses import asdict
from pathlib import Path

import pytest
from pandas import Timestamp

from nodestream.model import DesiredIngestion
from nodestream.pipeline import (
    PipelineInitializationArguments,
    PipelineProgressReporter,
)
from nodestream.project import PipelineDefinition


def set_default(obj):
    if isinstance(obj, frozenset):
        return list(obj)
    if isinstance(obj, Timestamp):
        return obj.isoformat()
    raise TypeError


def get_pipeline_fixture_file_by_name(name: str) -> Path:
    return Path("tests/integration/fixtures/pipelines") / name


@pytest.fixture
def drive_definition_to_completion():
    async def _drive_definition_to_completion(definition, **init_kwargs):
        results = []
        init_args = PipelineInitializationArguments(**init_kwargs)
        pipeline = definition.initialize(init_args)
        reporter = PipelineProgressReporter(
            reporting_frequency=1, callback=lambda _, record: results.append(record)
        )
        await pipeline.run(reporter)
        return [r for r in results if isinstance(r, DesiredIngestion)]

    return _drive_definition_to_completion


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("pipeline_name", ["fifa_2021_player_data.yaml"])
async def test_pipeline_interpretation_snapshot(
    snapshot, drive_definition_to_completion, pipeline_name, mocker
):
    from pandas import Timestamp

    mocked_ts = mocker.patch("pandas.Timestamp.utcnow")
    mocked_ts.return_value = Timestamp("2021-06-18")
    snapshot.snapshot_dir = "tests/integration/snapshots"
    pipeline_file = get_pipeline_fixture_file_by_name(pipeline_name)
    definition = PipelineDefinition.from_path(pipeline_file)
    results_as_json = json.dumps(
        [asdict(r) for r in (await drive_definition_to_completion(definition))],
        default=set_default,
        indent=4,
        sort_keys=True,
    )
    snapshot_file = f"interpretation_snapshot_{pipeline_name}.json"
    snapshot.assert_match(results_as_json, snapshot_file)
