# `ask` requires exactly one Target

`ask` must resolve exactly one Target before it performs schema discovery, query generation, validation, or execution. If multiple targets are configured and no target is selected, `ask` fails with a helpful list instead of silently choosing the first configured service, because natural-language query generation is schema-dependent and the wrong cluster/database can produce misleading or sensitive results.

## Considered Options

- Reuse the existing first-service default behavior everywhere.
- Let the model infer the target from natural language.
- Require exactly one resolved Target for `ask`.

## Consequences

`ask` may feel stricter than exact-KQL commands, but its failures are safer and clearer. A Target Catalog with stable aliases can recover convenience without allowing fuzzy target selection.
