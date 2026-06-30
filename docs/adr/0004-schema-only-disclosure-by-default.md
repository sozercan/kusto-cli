# Schema-only disclosure is the default

The default Data Disclosure Policy for `ask` sends compact Schema Context to the model provider but does not send raw sample rows or query results unless the user explicitly opts in. This trades some generation quality for a safer privacy baseline, while still allowing higher-context modes for users who intentionally permit additional data disclosure.

## Considered Options

- Send schema, samples, shots, and recent results by default.
- Send no target metadata to the model provider.
- Send compact Schema Context by default and require opt-in for sample data.

## Consequences

Generated queries may be less accurate for poorly documented schemas until users add shots, docstrings, or opt into samples. The default behavior is easier to explain and safer for clusters that may contain sensitive operational data.
