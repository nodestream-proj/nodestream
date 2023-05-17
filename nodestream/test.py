import json
from pathlib import Path

import pytest

from nodestream.project import RunRequest, Project, PipelineProgressReporter
from nodestream.pipeline import PipelineInitializationArguments


@pytest.fixture
def snapshot_pipeline(request):
    """A fixture that will snapshot the results of a pipeline run."""

    # request: https://docs.pytest.org/en/7.3.x/reference/reference.html#request
    test_name = request.node.name

    async def _snapshot_pipeline(pipeline_name, annotations=None, project_file=None):
        # Create a run request from the pipeline name and annotations with a reporting size of one.
        # The reporting size will be one so we can store a list of all records coming back from the pipeline.
        # the JSON serialize the list of returned results and store it in a snapshot file.
        # The snapshot file will be named after the test name and the pipeline name.
        # The snapshot file will be stored in the snapshots directory.
        results = []
        run_request = RunRequest(
            pipeline_name,
            initialization_arguments=PipelineInitializationArguments(
                annotations=annotations
            ),
            progress_reporter=PipelineProgressReporter(
                reporting_frequency=1, callback=lambda _, x: results.append(x)
            ),
        )
        project = Project.from_file(project_file)
        await project.run(run_request)

        snapshot_file = Path(f"tests/snapshots/{pipeline_name}/{test_name}.json")
        snapshot_file.parent.mkdir(parents=True, exist_ok=True)
        json.dump(snapshot_file, results)

    return _snapshot_pipeline
