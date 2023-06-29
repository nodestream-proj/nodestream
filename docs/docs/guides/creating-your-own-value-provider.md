# Creating A Value Provider

There are many methods of extracting and providing data to the ETl pipeline as it operates. The various yaml tags such
as `!jq` or `!variable` refer to an underlying `ValueProvider`. In order to introduce your own mechanism for
providing values you can create your own subclass of `ValueProvider`.


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

## Make sure your module is imported


```yaml
imports:
  - nodestream.databases.neo4j # an existing import
  - my_project.some_sub_package.value_providers
```


## Using your ValueProvider

This allows us to construct a value provider like so:


```yaml
some:
  place:
    in:
      the:
        pipeline: !hash
            hash_value: !variable foo
```


