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


### Additional Lifecycle Methods

In addition to the `activate` method, project plugins can optionally implement the following lifecycle methods: 

#### `def before_project_load(self, file_path: Path) -> None`

Called before a project is loaded. The `file_path` argument is the path to the project file.

#### `def after_project_load(self, project: Project) -> None`

Called after the project is loaded and all project plugins have been activated. 


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
        project.add_plugin_scope_from_pipeline_resources(name="my_plugin", package="my_plugin.pipelines")
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
    projects = "nodestream_plugin_cool.plugin"
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
        projects = nodestream_plugin_cool.plugin
    ```

=== "setup.py"
    ```python
    from setuptools import setup

    setup(
        # ...,
        entry_points = {
            'nodestream.plugins': [
                'projects = nodestream_plugin_cool.plugin',
            ]
        }
    )
    ```

### Configuration for Project Plugins

The `!config` YAML tag followed by a key can be used to provide end-user configurable values for project plugin pipelines.

#### Plugin Creator Provided
=== "plugin-pipeline.yaml"
    ```
    - implementation: myPlugin.testPipeline:TestExctractor
      arguments:
        base_url: !config 'service_base_url'
        username: !config 'service_username'
        password: !config 'service_password'
    ```

End users can provide values for the `!config` plugin tags in their nodestream.yaml file. This feature is particularly useful for supplying user-provided information such as URLs and credentials. The values can be accessed at plugin_config.<plugin_name>.<config_value>. To ensure proper provision of configuration values, the plugin name under `plugin_config` must match the plugin scope name. This enables plugins to use similar configuration value keys without conflicts.

#### Plugin End-User Provided
=== "nodestream.yaml"
    ```
    plugins:
      - name: myPlugin
        config:
          service_base_url: "https://mytestpluginapi.com"
          service_username: !env MY_TEST_PLUGIN_USERNAME
          service_password: !env MY_TEST_PLUGIN_PASSWORD
        targets:
          - target1
          - target2
      - name: otherPlugin
        config:
          service_base_url: "https://otherurl.com"
        targets:
          - target1
          - target2
    ```
#### Plugin Pipeline Configurations
To configure each plugin provided pipeline, end users can add a "pipelines" key under the imported plugin in their nodestream.yaml file. This configuration can include annotations, targets, and exclude_inherited_targets per pipeline. These properties are defined in the same way as those in pipelines under the scopes section.
=== "nodestream.yaml"
    ```
    plugins:
      - name: myplugin
        config:
          service_base_url: "https://otherurl.com"
        targets:
          - target1
          - target2
        pipelines:
            - name: plugin_pipeline_1
                exclude_inherited_targets: True
                annotations:
                    my_annoation: True
                targets:
                    - target3
    ```
