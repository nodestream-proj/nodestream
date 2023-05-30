# Creating Your Own Interpretation

Sometimes parsing data is extremely complex meaning its impossible to rely on the `Interpretation`
[DSL](https://en.wikipedia.org/wiki/Domain-specific_language) to handle every possible permutation of
different data. To handle this, the `Interpretation` system is pluggable. For more information on interpretations, see
the reference on interpreting data, see the [Creating a Graph ETL Pipeline](../creating-a-graph-etl-pipeline.md)

Here's an example. Let's say you want to store a boolean property but also want to store the negative property.
For instance, you want to store both an `enabled` and `disabled` property where
the one is the opposite value of the other.

## Define Your Interpretation Class

You can create a python file in your project, for example `interpretations.py`. If you generated your project from
`nodestream new project`, you will already have this file. Now let's write some code:

```python
from nodestream.interpreting import Interpretation

class MemoizeNegativeProperty(Interpretation, alias="memoize_negative"):
    pass
```

As you might imagine, this isn't particulary interesting. But, the `name="memoize_negative"` might have caught your eye.
`Interpretation`s are part of a unique registry. The `name` property corresponds with the `type` property that we covered in
the [Basic Usage](#basic-usage) section. Functionally, all other keys in the object are forwarded to this classes constructor.

Given that, let's consider our `MemoizeNegativeProperty` class. That implies that we could write down a constructor like this:

```python
from nodestream.interpreting import Interpretation
from nodestream.value_providers import ValueProvider

class MemoizeNegativeProperty(Interpretation, alias="memoize_negative"):
    def __init__(self, positive_name: str, negative_name, value: ValueProvider):
        # set properties
```

This code is type annotated. As you can see, `ValueProvider` is a new concept. A `ValueProvider` is something like `!jmespath` or
`!variable`. `nodestream` has a well documented model layer and its worth understanding the API that the model layer provides if you
start digging around more with extending nodestream.

Let's complete our implementation. Perhaps unsurprisingly, `Interpretation` subclasses need to implement an `interpret` method.
A working implemention of `MemoizeNegativeProperty` could look like this:

```python
from nodestream.interpreting import Interpretation
from nodestream.value_providers import ValueProvider

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

## Make sure your module is imported

Wherever you have your class defined, nodestream needs to know that its something that should be imported. To do
so, add your module to the imports section of your `nodestream.yaml` file. For example:

```yaml
imports:
  - nodestream.databases.neo4j # an existing import
  - my_project.some_sub_package.interpretations
```
