# Creating Your Own Audits

Audits are used to validate the state of a project. They are run via the `nodestream audit {{audit name}}` command. Audits are intended to be used to validate the state of a project, but can be used to enforce any arbitrary rules about the project including things like naming conventions, pipeline structure, or other project-specific rules.

## Creating an Audit

To create an audit, you must create a class that inherits from `nodestream.project.audits:Audit`. The class must implement the `run` method, which takes a single argument, `project`, and returns nothing. During the execution of the audit, you can call `self.failure` to indicate that the audit has failed or `self.warning` to indicate that there is a non-fatal issue with the project. Both methods take a single argument, `message`, which is a string describing the failure or warning.

```python
from nodestream.project import Project
from nodestream.project.audits import Audit

class MyAudit(Audit):
    name = "my-audit"

    def run(self, project: Project) -> None:
        pass
```

### Use Case: Enforcing Pipeline Naming Conventions

Audits can be used to enforce pipeline naming conventions. As an example, lets say we want to enforce that all pipelines in our project are named in snake case. We can do this by iterating over all of the pipelines in the project and checking that the name is in snake case. If it is not, we can call `self.failure` to indicate that the audit has failed.

```python
from nodestream.project import Project
from nodestream.project.audits import Audit

class MyAudit(Audit):
    name = "my-audit"

    def is_snake_case(self, name: str) -> bool:
        return name.replace("_", "").islower()

    def run(self, project: Project) -> None:
        for scope in project.scopes_by_name.values():
            for pipeline in scope.pipelines_by_name.values():
                if not self.is_snake_case(pipeline.name):
                    self.failure(f"Pipeline {pipeline.name} is not in snake case")
```

### Registering the Audit

Audits are registered via the [entry_points](https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins) API of a Python Package. Specifically, the `entry_point` named `audits` inside of the `nodestream.plugins` group is loaded. It is expected to be a subclass of `nodestream.project.audits:Audit` as directed above. An instance of the class is created and the `run` method is called with an instance of `nodestream.project:Project`.

The `entry_point` should be a module that contains at least one audit class. At runtime, the module will be loaded and all classes that inherit from `nodestream.project.audits:Audit` will be registered. The `name` attribute of the class will be used as the name of the audit.

Depending on how you are building your package, you can register your audit plugin in one of the following ways:

=== "pyproject.toml"
    ```toml
    [project.entry-points."nodestream.plugins"]
    audits = "nodestream_plugin_cool.audits"
    ```

=== "setup.cfg"
    ```ini
    [options.entry_points]
    nodestream.plugins =
        project = nodestream_plugin_cool.audits
    ```

=== "setup.py"
    ```python
    from setuptools import setup

    setup(
        # ...,
        entry_points = {
            'nodestream.plugins': [
                'project = nodestream_plugin_cool.audits',
            ]
        }
    )
    ```

## Running Audits

Audits are run via the `nodestream audit` command. The command takes a single argument, the name of the audit to run. The audit name is the `name` attribute of the audit class. For example, if we wanted to run the audit we created above, we would run `nodestream audit my-audit`.

The `nodestream audit` command takes the following options:

- `--project`: The path to the project file. Defaults to the current working directory's `nodestream.yaml` file.
