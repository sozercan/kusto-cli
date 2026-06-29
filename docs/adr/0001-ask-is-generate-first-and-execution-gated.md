# `ask` is generate-first and execution-gated

`ask` is a Query Draft Agent: by default it turns natural-language input into a Query Draft instead of running the generated KQL. Execution requires an explicit Execution Gate, such as `--execute`, because generated KQL should be inspectable before it touches a cluster and because convenience is less important than preventing surprising production reads.

## Considered Options

- Generate KQL only by default, with explicit execution.
- Automatically execute generated KQL.
- Build a broader autonomous Investigation Agent that explores and executes intermediate queries.

## Consequences

The first version optimizes for safety, auditability, and user trust over one-shot convenience. A future Investigation Agent can be added as a separate explicit mode without weakening the default `ask` contract.
