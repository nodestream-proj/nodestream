# Creating your own ArgumentResolver

A `ArgumentResolver` allows you to inline a value into the Pipeline file before the pipeline is initialzed. This can be
useful for passing configuration from files, environment, secret stores, and the like.

For example, assume that have a database password that you would like to retrieve from a secret store, in this case,
AWS secrets manager.

## Defining your ArgumentResolver Class

```python
from typing import Any

import boto3

from nodestream.argument_resolvers import ArgumentResolver


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
yaml to instanitate our new value provider. Nodestream uses [`pyyaml`](https://pyyaml.org/) to load our pipelines.

## Make sure your module is imported

Wherever you have your class defined, nodestream needs to know that its something that should be imported. To do
so, add your module to the imports section of your `nodestream.yaml` file. For example:

```yaml
imports:
  - nodestream.databases.neo4j # an existing import
  - my_project.some_sub_package.argument_resolvers
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
