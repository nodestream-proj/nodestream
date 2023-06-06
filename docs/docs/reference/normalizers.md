# Normalizers

A `Normalizer` allows you to clean data extracted by a [`ValueProvider`](./value-providers.md).
They are intended to provided stateless, simple transformations of data. Many different interpretations allow you to
enable `Normalizers` to apply these transformations. See the [`Interpretation`](./interpretations.md) reference for where
they can be applied.

## Normalizer Flags

| Normalizer Flag Name      | Example Input           | Example Output         |
| ------------------------- | ----------------------- | ---------------------- |
| `do_lowercase_strings`    | "dO_LoWER_cASe_strings" | "do_lowercase_strings" |
| `do_remove_trailing_dots` | "my.website.com."       | "my.website.com"       |
| `do_trim_whitespace`      | "  some value "         | "some value"           |
