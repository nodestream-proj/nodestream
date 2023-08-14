# Implemeting Pipeline Testing 

## Overview
Pipelines are the core of NodeStream. 
They are the building blocks that allow you to define your data processing logic.
Since so many parts of the application depend on pipelines. 
It is important to test them thoroughly to ensure that the integration between the different components is working as expected. 

Nodestream has some built-in tools to help you test your pipelines. See the [Project#get_snapshot_for](https://nodestream-proj.github.io/nodestream/python_reference/project/project/#nodestream.project.project.Project.get_snapshot_for) method for more details on running pipelines.

## Examples

### `pytest`

```python
# tests/test_pipelines.py

import dataclasses
import json

from nodestream.project import Project

from freezegun import freeze_time

# Step 1: Define a custom JSON encoder that will convert dataclasses to dicts.
class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)

# Step 2: Freeze time so that the snapshot is deterministic.
@freeze_time("2020-01-01")
def test_pipeline_snapshot(snapshot):
    # Step 3: Load your application project
    project = Project.read_from_file("nodestream.yaml")

    # Step 4: Run the pipeline and get the results gathered as a list.
    snapshot = project.get_snapshot_for("pipeline")
    snapshot_str = json.dumps(snapshot, cls=EnhancedJSONEncoder)

    # Step 5: Assert that the results match the snapshot
    #   Note: This will fail the first time you run it.
    snapshot.snapshot_dir = "snapshots"
    snapshot.assert_match(snapshot_str, "test_name_snapshot.json")
```