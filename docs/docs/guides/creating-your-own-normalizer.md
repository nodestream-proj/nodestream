# Creating A Normalizer

A `Normalizer` allows you to clean data extracted by a `ValueProvider`. They are intended to provided stateless, simple
transformations of data. Nodestream has some built in ones that you can view [here](../reference/normalizers.md).

For example, assume that you have numeric numbers that should be rounded to a whole number before being used.
Let's build a normalizer that does this for us.

## Defining your Normalizer Class

```python
from typing import Any

from nodestream.pipeline.normalizers import Normalizer

class RoundToWholeNumber(Normalizer, alias="round_numbers"):
    def normalize_value(self, value: Any) -> Any:
        return int(value) if isinstance(value, float) else value
```

## Registering your Normalizer

Normalizers are registered via the [entry_points](https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins) API of a Python Package. Specifically, the `entry_point` named `normalizers` inside of the `nodestream.plugins` group is loaded. Every Value Provider is expected to be a subclass of `nodestream.pipeline.normalizers:Normalizer` as directed above.

The `entry_point` should be a module that contains at least one `Normalizer` class. At runtime, the module will be loaded and all classes that inherit from `nodestream.pipeline.normalizers:Normalizer` will be registered.

Depending on how you are building your package, you can register your `Normalizer` plugin in one of the following ways:

=== "pyproject.toml"
    ```toml
    [project.entry-points."nodestream.plugins"]
    normalizers = "nodestream_plugin_cool.normalizers"
    ```

=== "setup.cfg"
    ```ini
    [options.entry_points]
    nodestream.plugins =
        normalizers = nodestream_plugin_cool.normalizers
    ```

=== "setup.py"
    ```python
    setup(
        ...
        entry_points={
            "nodestream.plugins": [
                "normalizers = nodestream_plugin_cool.normalizers"
            ]
        },
        ...
    )
    ```

## Using your Normalizer

You can now use your normalizer in sections that handle normalization flags. For more information,
see the [Interpretation's reference](../reference/interpretations.md). For example, if you are using this
in a source node interpretation, you could use it as follows:

```yaml
interpretations:
  - type: source_node
    node_type: Metric
    key:
      value: !jmespath value
    normalization:
      do_round_numbers: true
```

Note that the key for the flag is simply the concatenation of `round_numbers` from the `Normalizer` name and `do_`
