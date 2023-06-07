# Creating A Transformer

A transformer is a class that takes a record as input and yields one or more output records. They are intended for pre-interpretation data transformations.

## Defining A Transformer

To define a transformer, you must create a class that inherits from `nodestream.pipeline:Transformer`. The class must implement the `transform_record` method, which takes a single argument, `record`, and yields one or more output records.

For example, the following transformer takes a record with the following shape:

```json
{
    "team": "Data Science",
    "people": [
        {"name": "Joe", "age": 25},
        {"name": "John", "age": 34},
    ]
}
```

and yields the following records:

```json
{
    "name": "Joe",
    "age": 25,
    "team": "Data Science"
}
```

```json
{
    "name": "John",
    "age": 34,
    "team": "Data Science"
}
```

The transformer is defined as follows:

```python
from nodestream.pipeline import Transformer

class FlattenPeopleTransformer(Transformer):
    def __init__(self, people_field):
        self.people_field = people_field

    def transform_record(self, record):
        team = record["team"]
        for person in record[self.people_field]:
            person["team"] = team
            yield person
```

## Using A Transformer

To use a transformer, you must add it to your pipeline configuration. For example, the following pipeline configuration uses the `FlattenPeopleTransformer` defined above:

```yaml
- implementation: my_module:FlattenPeopleTransformer
  arguments:
    people_field: people

# pipeline to be continued...
```