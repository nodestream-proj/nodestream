import json
from dataclasses import asdict
from pathlib import Path
from typing import Union

import pytest
from pandas import Timestamp

from nodestream.model import DesiredIngestion, KeyIndex, FieldIndex
from nodestream.pipeline import PipelineInitializationArguments, Pipeline
from nodestream.project import PipelineDefinition


def set_default(obj):
    if isinstance(obj, frozenset):
        return list(obj)
    if isinstance(obj, Timestamp):
        return obj.isoformat()
    raise TypeError


def get_pipeline_fixture_file_by_name(name: str) -> Path:
    return Path("tests/integration/fixtures/pipelines") / name


async def get_pipeline_outputs(
    pipeline: Pipeline,
) -> Union[DesiredIngestion, KeyIndex, FieldIndex]:
    return [r async for r in pipeline.run()]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pipeline_interpretation_snapshot(snapshot):
    pipeline_name = "fifa_2021_player_data.yaml"
    snapshot.snapshot_dir = "tests/integration/snapshots"
    pipeline_file = get_pipeline_fixture_file_by_name(pipeline_name)
    definition = PipelineDefinition.from_path(pipeline_file)
    pipeline = definition.initialize(PipelineInitializationArguments())
    results = await get_pipeline_outputs(pipeline)
    results_as_json = json.dumps(
        [asdict(r) for r in results], default=set_default, indent=4, sort_keys=True
    )
    snapshot_file = f"interpretation_snapshot_{pipeline_name}.json"
    snapshot.assert_match(results_as_json, snapshot_file)
