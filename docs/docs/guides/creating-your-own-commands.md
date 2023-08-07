# Creating your own Commands

Commands are used to interact with nodestream from the command line.

## Creating your own command

Commands are implemented as [cleo](https://cleo.readthedocs.io/en/latest/usage.html) commands via a subclass of `nodestream.cli.commands:NodestreamCommand`.

```python
from nodestream.cli.commands import NodestreamCommand

class MyCommand(NodestreamCommand):
    name = "my-command"
    description = "My command description"

    async def handle_async(self) -> None:
        self.line("Hello World!")
```

### Operations

Commands are typically executed as a series of operations. Operations are defined as seperate classes that from `nodestream.cli.operations:Operation`.

```python
from nodestream.cli.command import NodestreamCommand
from nodestream.cli.operations import Operation

class MyOperation(Operation):
    async def perform(self, command: NodestreamCommand):
        command.line("Hello World!")
```

And then used as part of a command like so:

```python
from nodestream.cli.commands import NodestreamCommand

class MyCommand(NodestreamCommand):
    name = "my-command"
    description = "My command description"

    async def handle_async(self) -> None:
        operation = MyOperation()
        await self.run_operation(operation)
```

### Command Arguments

Commands can take arguments and options from the command line. These are defined as class attributes on the command class.

```python
from cleo.helpers import argument, option

from nodestream.cli.commands import NodestreamCommand

class MyCommand(NodestreamCommand):
    name = "my-command"
    description = "My command description"
    arguments = [
        argument("name", "the name of the project to create"),
    ]

    options = [
        option("shout", "s", "Yell the message"),
    ]

    async def handle_async(self) -> None:
        name = self.argument("name")
        message = f"Hello {name}!"
        if self.option("shout"):
            message = message.upper()
        self.line(message)
```


### Registering the Command

Commands are registered via the [entry_points](https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins) API of a Python Package. Specifically, the `entry_point` named `commands` inside of the `nodestream.plugins` group is loaded. All commands are expected to be a subclass of `nodestream.cli.commands:NodestreamCommand` as directed above.

The `entry_point` should be a module that contains at least one command class. At runtime, the module will be loaded and all classes that inherit from `nodestream.cli.commands:NodestreamCommand` will be registered. The `name` and `description` attributes of the class will be used as as the came and description of the command used in the command line and its help output.

Depending on how you are building your package, you can register your command plugin in one of the following ways:

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

## Running a command

Commands are then run via the `nodestream` command line tool.

```bash
$ nodestream my-command
Hello World!
```
