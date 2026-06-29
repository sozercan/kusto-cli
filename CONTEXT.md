# kusto-cli

This context describes the product language for `kusto-cli`, a standalone CLI for Kusto workflows and agent-friendly query generation.

## Language

**Query Draft Agent**:
The natural-language feature that turns a user request into a proposed read-only KQL query, with assumptions and validation metadata. It does not autonomously investigate or execute unless explicitly asked.
_Avoid_: Investigation Agent, autonomous agent, auto-query agent

**Target**:
The single cluster/database pair used by a Query Draft Agent for schema discovery, query generation, validation, and optional execution.
_Avoid_: endpoint, selected service, environment

**Target Catalog**:
The configured set of named Kusto targets available to `kusto-cli`.
_Avoid_: known-services, clusters list

**Query Draft**:
A proposed KQL query plus assumptions, warnings, selected schema context, and validation metadata.
_Avoid_: generated answer, final answer

**Execution Gate**:
The explicit user action or flag required before a Query Draft is executed against Kusto.
_Avoid_: auto-run, implicit execution

**Schema Context**:
The compact table, column, type, docstring, and function metadata supplied to the Query Draft Agent.
_Avoid_: schema dump, database snapshot

**Data Disclosure Policy**:
The rule set controlling what target metadata or data may be sent to a model provider.
_Avoid_: privacy mode, sample setting

**Repair Pass**:
A bounded attempt to fix a generated query using validation errors and schema context.
_Avoid_: autonomous retry loop
