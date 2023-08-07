# Transformers

## `ValueProjection`

Uses a `ValueProvider` to project value(s) from the input record. For example:

```yaml
- implementation: nodestream.transformers:ValueProjection
  arguments:
    projection: !jmespath "items[*]"
```

Given this input:

```json
{"items": [{"index": 1}, {"index": 2}, {"index": 3}]}
```

Produces these output records:

```json
{"index": 1}
{"index": 2}
{"index": 3}
```