# Value Providers

There are many methods of extracting and providing data to the ETl pipeline as it operates.
The various yaml tags such as `!jmespath` or `!variable` refer to an underlying ValueProvider.

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
    # other interpretations are omitted.
    - type: source_node
      node_type: Person
         name: !variable name
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

The `!format` value provider allows you to format a string using the `format` method. For example, if you wanted to create a hello world
node based on a name field in the record, you could do the following:

```json
{
    "name": "Joe",
    "age": 25
}
```

The following interpretation would create a node with the key `Hello, Joe!`:

```yaml
- implementation: nodestream.interpreting:Interpreter
  arguments:
    interpretations:
      - type: source_node
        node_type: HelloNode
        key:
          name: !format 
            fmt: "Hello, {name}!"
            name: !jmespath name
        properties:
          age: !jmespath age
```

## `!regex`

The `!regex` value provider allows you to extract a value from a string using a regular expression. For example, if you wanted to extract
the first name from a string given a record like this:

```json
{
    "name": "Joe Smith",
    "age": 25
}
```

The following interpretation would create a node with the key `Joe`:

```yaml
- implementation: nodestream.interpreting:Interpreter
  arguments:
    interpretations:
      - type: source_node
        node_type: HelloNode
        key:
          first_name: !regex 
            regex: "^(?P<first_name>[a-zA-Z]+)\s(?P<last_name>[a-zA-Z]+)$"
            data: !jmespath name
            group: first_name
        properties:
          age: !jmespath age
```

You can either use named groups or numbered groups.
If you use named groups, you can specify the group name in the `group` argument. 
If you use numbered groups, you can specify the group number in the `group` argument. 
If you do not specify a group, the first group will be used - which is the entire match.

