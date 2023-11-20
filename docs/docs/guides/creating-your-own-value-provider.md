# Creating A Value Provider

There are many methods of extracting and providing data to the ETl pipeline as it operates. The various yaml tags such
as `!jmespath` or `!variable` refer to an underlying `ValueProvider`.

## Creating a Value Provider
In order to introduce your own mechanism for providing values you can create your own subclass of `ValueProvider`.

```python
from nodestream.pipeline.value_providers import ValueProvider

class HashValueProvider(ValueProvider):
    pass

```


```python
from nodestream.pipeline.value_providers import ValueProvider, ProviderContext

class HashValueProvider(ValueProvider):
    def single_value(self, context: ProviderContext) -> Any:
        ...

    def many_values(self, context: ProviderContext) -> Iterable[Any]:
        ....
```



```python
from typing import Any, Iterable

from nodestream.pipeline.value_providers import ValueProvider, ProviderContext
from some_hashing_library import hash_value


class HashValueProvider(ValueProvider):
    def __init__(self, value_provider_to_hash: ValueProvider):
        self.value_provider_to_hash = value_provider_to_hash

    def single_value(self, context: ProviderContext) -> Any:
        return hash_value(self.value_provider_to_hash.single_value(context))

    def many_values(self, context: ProviderContext) -> Iterable[Any]:
        for value in self.value_provider_to_hash.many_values(context):
            yield hash_value(value)
```

Now that we have implemented the hashing behavior, we'd like to use it. However, currently our `HashValueProvider` is
not constructable in our pipelines. To accomplish this, we need to register a yaml loader that can register a tag in
yaml that can instantiate our new value provider. Nodestream uses [`pyyaml`](https://pyyaml.org/) to load our pipelines.
For our purposes, our loader can be created by doing the following:

```python
# other imports omitted

from typing import Type

from yaml import SafeLoader


class HashValueProvider(ValueProvider):
    # remainder of implementation omitted.

    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!hash", lambda loader, node: cls(value_provider_to_hash=loader.construct_mapping(node)["hash_value"])
        )
```

## Registering the Value Provider

ValueProviders are registered via the [entry_points](https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins) API of a Python Package. Specifically, the `entry_point` named `value_providers` inside of the `nodestream.plugins` group is loaded. Every Value Provider is expected to be a subclass of `nodestream.pipeline.value_providers:ValueProvider` as directed above.

The `entry_point` should be a module that contains at least one Value Provider class. At runtime, the module will be loaded and all classes that inherit from `nodestream.pipeline.value_providers:ValueProvider` will be registered.

Depending on how you are building your package, you can register your Value Provider plugin in one of the following ways:

=== "pyproject.toml"
    ```toml
    [project.entry-points."nodestream.plugins"]
    value_providers = "nodestream_plugin_cool.value_providers"
    ```

=== "pyproject.toml (poetry)"
    ```toml
    [tool.poetry.plugins."nodestream.plugins"]
    value_providers = "nodestream_plugin_cool.value_providers"
    ```

=== "setup.cfg"
    ```ini
    [options.entry_points]
    nodestream.plugins =
        value_providers = nodestream_plugin_cool.value_providers
    ```

=== "setup.py"
    ```python
    setup(
        ...
        entry_points={
            "nodestream.plugins": [
                "value_providers = nodestream_plugin_cool.value_providers"
            ]
        },
        ...
    )
    ```

## Debuggability 

`nodestream` contains a feature that allows the user to print the effective pipeline configuration. This is useful for debugging purposes. 
This means that `nodestream` needs to understand how to represent your custom value provider in a human readable way in yaml. 
To do this, you need to implement a `Representer` for your value provider. 

```python
from yaml import SafeDumper

class HashValueProvider(ValueProvider):
    # ... implementation omitted


SafeDumper.add_representer(
    HashValueProvider,
    lambda dumper, hash: dumper.represent_scalar(
        "!hash", hash.value_provider_to_hash
    ),
)
