# Value Providers

There are many methods of extracting and providing data to the ETl pipeline as it operates.
The various yaml tags such as `!jq` or `!variable` refer to an underlying ValueProvider.

## `!jmespath`

Represents a [jmespath](https://jmespath.org/) query language expression that should be executed against the input record.

For example, if you want to get extract all of the `name` fields from the list of people provided in a document like this:

```json
{
    "people": [{"name": "Joe", "age": 25}, {"name": "john", "age": 45}]
}
```

A valid `!jmespath` value provider would look like this: `!jmespath people[*].name` Essentially, any `jmespath` expression
provided after the `!jmespath` tag will be parsed and loaded as one. Another guide on `jmespath` can be found [here](https://jmespath.site/main/).

## `!jq`

Represents a [jq](https://jqlang.github.io/jq/) query language expression that should be executed agains the input record.

For example, if you want to get extract all of the `name` fields from the list of people provided in a document like this:

```json
{
    "people": [{"name": "Joe", "age": 25}, {"name": "john", "age": 45}]
}
```

A valid `!jq` value provider would look like this: `!jq .people[].name` Essentially, any `jq` expression
provided after the `!jq` tag will be parsed and loaded as one. More information on `jq` can be found [here](https://jqlang.github.io/jq/tutorial/).

## `!variable`

Provides the value of an extracted variable from the [Variables Interpretation](./interpretations.md#variables-interpretation). For instance, if
you provided an variables interpretation block like so:

```yaml
interpretations:
    - type: variables
      variables:
         name: !jmespath person.name
```

You are then able to use the `!variable` provided in a later interpretation. For example,

```yaml
interpretations:
    # other interpretations are ommitted.
    - type: source_node
      node_type: Person
         name: !variable namee
```

This is particularly helpful when using the `before_iteration` and `iterate_on` clause in an `Interpreter`. For example,
assume that you have a record that looks like this:

```json
{
    "team_name": "My Awesome Team",
    "people": [
        {"name": "Joe", "age": 25},
        {"name": "John", "age": 34},
    ]
}
```

On way to ingest this data would be to do the following:

```yaml
- implementation: nodestream.interpreting:Interpreter
  arguments:
    before_iteration:
      - type: variables
        variables:
           team: !jmespath team
    iterate_on: !jmespath people[]
    interpretations:
      - type: source_node
        node_type: Person
        key:
          name: !jmespath name
        properties:
          age: !jmespath age
      - type: relationship
        node_type: Team
        relationship_type: PART_OF
        node_key:
          name: !variable team
```


## `!format`

TODO
