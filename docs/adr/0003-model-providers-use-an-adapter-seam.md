# Model providers use an adapter seam

The Query Draft Agent will depend on a model-provider adapter seam rather than directly embedding one provider's request and response format into command handling. This keeps the natural-language feature testable with fake adapters and leaves room for OpenAI-compatible endpoints, Azure OpenAI, local models, or future providers without rewriting the agent workflow.

## Considered Options

- Hard-code one model provider into the `ask` command.
- Implement several providers directly in command handling.
- Place provider-specific behavior behind an adapter seam.

## Consequences

The initial implementation has a little more structure than a single-provider spike, but provider lock-in and command-handler complexity stay low.
