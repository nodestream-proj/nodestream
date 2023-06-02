# Creating A Normalizer

A `Normalizer` allows you to clean data extracted by a `ValueProvider`. They are intended to provided stateless, simple
transformations of data. Nodestream has some built in ones that you can view [here](../reference/normalizers.md).

For example, assume that you have numeric numbers that should be rounded to a whole number before being used.
Let's build a normalizer that does this for us.

## Defining your Normalizer Class

```python
from typing import Any

from nodestream.normalizers import Normalizer

class RoundToWholeNumber(Normalizer, alias="round_numbers"):
    def normalize_value(self, value: Any) -> Any:
        return int(value) if isinstance(value, float) else value
```

## Make sure your module is imported

Wherever you have your class defined, nodestream needs to know that its something that should be imported. To do
so, add your module to the imports section of your `nodestream.yaml` file. For example:

```yaml
imports:
  - nodestream.databases.neo4j # an existing import
  - my_project.some_sub_package.normalizers
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

Note that the key for the flag is simply the concatination of `round_numbers` from the `Normalizer` name and `do_`
