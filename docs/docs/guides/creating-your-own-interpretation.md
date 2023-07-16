# Creating A New Interpretation

Sometimes parsing data is extremely complex meaning its impossible to rely on the `Interpretation`
[DSL](https://en.wikipedia.org/wiki/Domain-specific_language) to handle every possible permutation of
different data. To handle this, the `Interpretation` system is plug-able.

Here's an example. Let's say you want to store a boolean property but also want to store the negative property.
For instance, you want to store both an `enabled` and `disabled` property where
the one is the opposite value of the other.

For more information on interpretations see the [Tutorial](../tutorial.md) and the [Interpreter Reference](../reference/interpreter.md).

## Define Your Interpretation Class

You can create a python file in your project, for example `interpretations.py`. If you generated your project from
`nodestream new project`, you will already have this file. Now let's write some code:

```python
from nodestream.interpreting import Interpretation

class MemoizeNegativeProperty(Interpretation, alias="memoize_negative"):
    pass
```

As you might imagine, this isn't particularly interesting. But, the `name="memoize_negative"` might have caught your eye.
`Interpretation`s are part of a unique registry. The `alias` property corresponds with the `type` property that is covered in
the [Interpreter Reference](../reference/interpreter.md) section. Functionally, all other keys in the object are forwarded to this classes constructor.

Given that, let's consider our `MemoizeNegativeProperty` class. That implies that we could write down a constructor like this:

```python
from nodestream.interpreting import Interpretation
from nodestream.pipeline.value_providers import ValueProvider

class MemoizeNegativeProperty(Interpretation, alias="memoize_negative"):
    def __init__(self, positive_name: str, negative_name, value: ValueProvider):
        # set properties
```

This code is type annotated. As you can see, `ValueProvider` is a new concept. A `ValueProvider` is something like `!jmespath` or
`!variable`. `nodestream` has a well documented model layer and its worth understanding the API that the model layer provides if you
start digging around more with extending nodestream.

Let's complete our implementation. Perhaps unsurprisingly, `Interpretation` subclasses need to implement an `interpret` method.
A working implementation of `MemoizeNegativeProperty` could look like this:

```python
from nodestream.interpreting import Interpretation
from nodestream.pipeline.value_providers import ValueProvider

class MemoizeNegativeProperty(Interpretation, alias="memoize_negative"):
    # __init__ omitted

    def interpret(self, context: InterpretationContext):
        source_node_properties = context.desired_ingest.source.properties
        actual_value = self.value.provide_single_value(context)
        source_node_properties.set_property(self.positive_name, actual_value)
        source_node_properties.set_property(self.negative_name, not actual_value)
```

Again, it will be valuable to read the API details on nodestream's model.
The above code leverages the aforementioned `InterpretationContext` as well as `DesiredIngest` and `PropertySet`.

## Register Your Interpretation


Interpretations are registered via the [entry_points](https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins) API of a Python Package. Specifically, the `entry_point` named `interpretations` inside of the `nodestream.plugins` group is loaded. Every `Interpretation`  is expected to be a subclass of `nodestream.interpreting:Interpretation` as directed above.

The `entry_point` should be a module that contains at least one `Interpretation` class. At runtime, the module will be loaded and all classes that inherit from `nodestream.interpreting:Interpretation` will be registered. The `alias` attribute of the class will be used as as the name of the tag used in the yaml pipeline.

Depending on how you are building your package, you can register your `Interpretation` plugin in one of the following ways:

=== "pyproject.toml"
    ```toml
    [project.entry-points."nodestream.plugins"]
    interpretations = "nodestream_plugin_cool.interpretations"
    ```

=== "setup.cfg"
    ```ini
    [options.entry_points]
    nodestream.plugins =
        interpretations = nodestream_plugin_cool.interpretations
    ```

=== "setup.py"
    ```python
    setup(
        ...
        entry_points={
            "nodestream.plugins": [
                "interpretations = nodestream_plugin_cool.interpretations"
            ]
        },
        ...
    )
    ```

## Use Your Interpretation

Now that you've defined your interpretation, you can use it in your pipeline. For example:

```yaml
# ... other pipeline steps
- implementation: nodestream.interpreting:Interpreter
  arguments:
    interpretations:
      # ... other interpretations
      - type: memoize_negative
        positive_name: enabled
        negative_name: disabled
        value: !jmespath "enabled"
```
