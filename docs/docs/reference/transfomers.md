# Transformers

## `ValueProjection`

Uses a `ValueProvider` to project value(s) from the input record. For example:

```yaml
- implementation: nodestream.transformers:ValueProjection
  arguments:
    projection: !jmespath "items[*]"
    additional_values:
      metadata: !jmespath "metadata"
```

Given this input:

```json
{"metadata": "stuff", "items": [{"index": 1}, {"index": 2}, {"index": 3}]}
```

Produces these output records:

```json
{"metadata": "stuff", "index": 1}
{"metadata": "stuff", "index": 2}
{"metadata": "stuff", "index": 3}
```

Adding additional key value Jmespath pairs will include different data within the records.
The base ValueProjection implementation will only keep the context of the values being iterated on.

## `SwitchTransformer`

Uses a `SwitchTansformer` to conditionally run different transformer methods for different record formats coming from a stream.

```yaml
- implementation: nodestream.transformers:SwitchTransformer
  arguments:
    switch_on: !jmespath "type"
    cases: 
      multiple:
        implementation: nodestream.transformers:ValueProjection
        arguments:
          projection: !jmespath "values[*]"
          additional_values:
            type: !jmespath "type"
```

Given these inputs:

```json
{"type": "single", "value": "1"}
{"type": "multiple", "values": {"value": "2", "value": "3", "value": "4"}}
```

Produces these output records:

```json
{"type": "single", "value": "1"}
{"type": "multiple", "value": "2"}
{"type": "multiple", "value": "3"}
{"type": "multiple", "value": "4"}
```