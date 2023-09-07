# Creating An ArgumentResolver

A `ArgumentResolver` allows you to inline a value into the Pipeline file before the pipeline is initialized. This can be
useful for passing configuration from files, environment, secret stores, and the like.

For example, assume that have a database password that you would like to retrieve from a secret store, in this case,
AWS secrets manager.

## Defining your ArgumentResolver Class

```python
from typing import Any

import boto3

from nodestream.pipeline.argument_resolvers import ArgumentResolver


class EnvironmentResolver(ArgumentResolver):
    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!secrets_manager",
            lambda loader, node: cls.get_from_secrets_manager(loader.construct_scalar(node)),
        )

    @staticmethod
    def get_from_secrets_manager(variable_name):
        client = boto3.client("secretsmanager")
        return client.get_secret_value(SecretId=variable_name)["SecretString"]

```

Note that this implementation is pretty naive. But it's the simplest we need to demonstrate the point.

In this example, we register with a yaml loader that can load a tag in
yaml to utilize our new `ArgumentResolver`. Nodestream uses [`pyyaml`](https://pyyaml.org/) to load our pipelines.

## Registering your ArgumentResolver

ArgumentResolvers are registered via the [entry_points](https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins) API of a Python Package. Specifically, the `entry_point` named `argument_resolvers` inside of the `nodestream.plugins` group is loaded. It is expected to be a subclass of `nodestream.pipeline.argument_resolvers:ArgumentResolver` as directed above.

The `entry_point` should be a module that contains at least one argument resolver class. At runtime, the module will be loaded and all classes that inherit from `nodestream.pipeline.argument_resolvers:ArgumentResolver` will be registered.

Depending on how you are building your package, you can register your Argument Resolver plugin in one of the following ways:

=== "pyproject.toml"
    ```toml
    [project.entry-points."nodestream.plugins"]
    argument_resolvers = "nodestream_plugin_cool.argument_resolvers"
    ```

=== "pyproject.toml (poetry)"
    ```toml
    [tool.poetry.plugins."nodestream.plugins"]
    argument_resolvers = "nodestream_plugin_cool.argument_resolvers"
    ```

=== "setup.cfg"
    ```ini
    [options.entry_points]
    nodestream.plugins =
        argument_resolvers = nodestream_plugin_cool.argument_resolvers
    ```

=== "setup.py"
    ```python
    from setuptools import setup

    setup(
        # ...,
        entry_points = {
            'nodestream.plugins': [
                'argument_resolvers = nodestream_plugin_cool.argument_resolvers',
            ]
        }
    )
    ```

## Using your ArgumentResolver

You can now use your `ArgumentResolver` anywhere in the pipeline document. For instance, lets configure our database
with the password:

```yaml
- implementation: nodestream.databases:GraphDatabaseWriter
  arguments:
    batch_size: 1000
    database: neo4j
    uri: bolt://localhost:7687
    username: neo4j
    password: !secrets_manager my_neo_db_password
```
