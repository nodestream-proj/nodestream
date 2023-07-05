# Creating Plugins

## Project Plugins
TODO

### Creating a Project Plugin

TODO

### Use Case: Distributing Pipelines

TODO

### Registering the Project Plugin

Application plugins are registered via the [entry_points](https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins) API of a Python Package.
Specifically, the `entry_point` named `project` inside of the `nodestream.plugins` group is loaded.
It is expected to be a subclass of `nodestream.application:ProjectPlugin` as directed above.
An instance of the class is created and the `activate` method is called with an instance of `nodestream.project:Project`.

=== "pyproject.toml"
    ```toml
    [project.entry-points."nodestream.plugins"]
    project = "nodestream_plugin_cool:MyProjectPlugin"
    ```

=== "setup.cfg"
    ```ini
    [options.entry_points]
    nodestream.plugins =
        project = nodestream_plugin_cool:MyProjectPlugin
    ```


=== "setup.py"
    ```python
    from setuptools import setup

    setup(
        # ...,
        entry_points = {
            'nodestream.plugins': [
                'project = nodestream_plugin_cool:MyProjectPlugin',
            ]
        }
    )
    ```


## Application Plugins

Application Plugins are responsible for adding additional commands and behavior to the nodestream application (the CLI).
Namely, one can either directly add a new command like `new` or `show`, or by adding an `Audit` to the application.

### Creating an Application Plugin

To create an an application plugin, you need to subclass from `ApplicationPlugin` and implement the `activate` method.

```python
from nodestream.application import Nodestream, ApplicationPlugin


class MyAwesomePlugin(ApplicationPlugin):
    def activate(self, nodestream: Nodestream):
        # TODO: do stuff like nodestream like call `add_command` or `add_audit`.
        pass

```

### Use Case: Adding a Command

TODO

### Use Case: Adding an Audit

TODO

### Registering the Application Plugin

Application plugins are registered via the [entry_points](https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins) API of a Python Package.
Specifically, the`entry_point` named `application` inside of the `nodestream.plugins` group is loaded.
It is expected to be a subclass of `nodestream.application:ApplicationPlugin` as directed above.
An instance of the class is created and the `activate` method is called with an instance of `nodestream.application:Nodestream`.

=== "pyproject.toml"
    ```toml
    [project.entry-points."nodestream.plugins"]
    application = "nodestream_plugin_cool:MyApplicationPlugin"
    ```

=== "setup.cfg"
    ```ini
    [options.entry_points]
    nodestream.plugins =
        application = nodestream_plugin_cool:MyApplicationPlugin
    ```


=== "setup.py"
    ```python
    from setuptools import setup

    setup(
        # ...,
        entry_points = {
            'nodestream.plugins': [
                'application = nodestream_plugin_cool:MyApplicationPlugin'
            ]
        }
    )
    ```
