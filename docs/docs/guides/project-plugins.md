# Project Plugins

Project plugins are used to modify, extend, or in otherwise interact with a project. They are loaded when a project is created or opened. 
They are intended for things like adding custom metadata to project pipelines, distributing additional piplines, or other project-oriented tasks.

## Creating a Project Plugin

To create a project plugin, you must create a class that inherits from `nodestream.project:ProjectPlugin`. The class must implement the `activate` method, which takes a single argument, `project`, and returns nothing. The `project` argument is an instance of `nodestream.project:Project`.

```python
from nodestream.project import Project, ProjectPlugin

class MyProjectPlugin(ProjectPlugin):
    def activate(self, project: Project) -> None:
        pass
```

### Use Case: Adding Custom Metadata to Project Pipelines

Project plugins can be used to add custom metadata to project pipelines. This metadata can be used for a variety of purposes specific to your 
application. As an example, lets say we want to set cron-like schedules for our project pipelines. We can do this by adding a `schedule` field to
the pipeline metadata. We can then use this metadata to schedule our pipelines to run at the appropriate times. As an example, we could search 
for any pipeline whose `name` contains `daily` and add a `schedule` annotation to it.

```python
from nodestream.project import Project, ProjectPlugin

class MyProjectPlugin(ProjectPlugin):
    def activate(self, project: Project) -> None:
        for scope in project.scopes_by_name.values():
            for pipeline in scope.pipelines_by_name.values():
                if 'daily' in pipeline.name:
                    pipeline.annotations['schedule'] = '0 0 * * *'

```

### Use Case: Distributing Additional Pipelines

Project plugins can be used to distribute additional pipelines with your application. As an example, lets say we want to distribute a set of 
pipelines that interact with a specific API. These pipelines can be packaged as a plugin and distributed with `pypi` as a seperate package. The plugin can then be registered as a project plugin, and the pipelines will be added to any project that is opened.

To support this, `PipelineScope` has a factory method that can be used to create an instance using the 
`importlib.resources` api. This allows you to distribute pipelines as part of a python package, and load them into a project at runtime.

```python
from nodestream.project import Project, ProjectPlugin, PipelineScope, PipelineDefinition

class MyProjectPlugin(ProjectPlugin):
    def activate(self, project: Project) -> None:
        scope = PipelineScope.from_resources(name="my_plugin", package="my_plugin.pipelines")
        project.add_scope(scope)
```

### Registering the Project Plugin

Project plugins are registered via 
the [entry_points](https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins) API of a Python Package.
Specifically, the `entry_point` named `project` inside of the `nodestream.plugins` group is loaded.
All project plugins are expected to be a subclass of `nodestream.project:ProjectPlugin` as directed above.
An instance of the class is created and the `activate` method is called with an instance of `nodestream.project:Project`.

The `entry_point` should be a module that contains at least one `ProjectPlugin` class. At runtime, the module will be loaded and all classes that inherit from `nodestream.project:ProjectPlugin` will be registered.

Depending on how you are building your package, you can register your project plugin in one of the following ways:

=== "pyproject.toml"
    ```toml
    [project.entry-points."nodestream.plugins"]
    projects = "nodestream_plugin_cool:plugin"
    ```

=== "pyproject.toml (poetry)"
    ```toml
    [tool.poetry.plugins."nodestream.plugins"]
    projects = "nodestream_plugin_cool.plugin"
    ```

=== "setup.cfg"
    ```ini
    [options.entry_points]
    nodestream.plugins =
        projects = nodestream_plugin_cool:MyProjectPlugin
    ```

=== "setup.py"
    ```python
    from setuptools import setup

    setup(
        # ...,
        entry_points = {
            'nodestream.plugins': [
                'projects = nodestream_plugin_cool:MyProjectPlugin',
            ]
        }
    )
    ```
