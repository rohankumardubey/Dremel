# SQL support

Both engines implement the same typed SQL surface over the generated `events`,
`users`, and `campaigns` tables. Unsupported syntax is rejected instead of
being interpreted differently by each implementation.

## Data types and expressions

- `NULL`, `BOOLEAN`, `INT64`, `DOUBLE`, `DECIMAL(18,2)`, `VARCHAR`, `DATE`,
  UTC `TIMESTAMP`, and second-granularity `INTERVAL`
- Checked integer and scaled-decimal arithmetic
- Comparisons and SQL three-valued boolean logic
- `CASE`, `IN`, `BETWEEN`, `LIKE`, and explicit `CAST`
- `COALESCE`, `NULLIF`, `ABS`, `LOWER`, `UPPER`, `LENGTH`, `SUBSTRING`,
  `CONCAT`, `DATE`, `DATE_TRUNC`, and `EXTRACT`

Invalid casts, integer overflow, and division by zero produce `NULL`. The only
implicit numeric widening is `INT64 -> DECIMAL -> DOUBLE`.

## Queries

- Inner, left, right, full, and cross joins, including multi-way joins
- `WHERE`, projection, `DISTINCT`, `GROUP BY`, `HAVING`, and aggregates
- Multi-key `ORDER BY`, explicit null placement, `LIMIT`, and `OFFSET`
- Non-recursive CTEs and derived tables
- Scalar, `EXISTS`, and `IN` subqueries, including correlated subqueries
- `UNION` and `UNION ALL`
- `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, and `LEAD`
- Partitioned aggregate windows and running unbounded-preceding frames

The binder resolves aliases and qualified names before execution, rejects
ambiguous columns, checks aggregate and window placement, and assigns a result
type to every expression.

## Optimizer

Prepared plans record row estimates, statistics, and applied rules. The shared
optimizer supports constant folding, three-valued-logic simplification,
predicate pushdown, projection pruning, filter ordering, Top-K replacement,
safe inner-join reordering, hash-join selection, build-side selection, and
hash-probe runtime filters.

Set `DREMEL_DISABLE_OPTIMIZER=1` to build a comparable unoptimized plan.
`EXPLAIN` shows physical operators, estimates, join choices, and applied rules.

## Concurrent execution

The long-lived benchmark server has a bounded admission queue, global and
per-group memory limits, active-query limits, deadlines, cancellation, and a
4:2:1 priority-weighted scheduler. FIFO order is preserved within each
priority/resource group. Each admitted request's memory reservation is also
its hard query workspace cap. The concurrency benchmark reports throughput,
outcomes, queue and execution latency percentiles, and Jain's fairness index.

## Memory-bounded execution

`--query-memory-limit-mb` applies a query-scoped budget shared by all worker
threads. Both engines account scan selections, hash aggregation
tables, join build tables and outputs, distinct sets, window state,
intermediate relations, top-k buffers, and result materialization. Accounting
is monotonic for the lifetime of a query, so it can reject a query even when an
earlier operator's allocation is no longer live. The value is an operator
accounting metric, not process RSS. Budget exhaustion returns
`RESOURCE_EXHAUSTED` without returning a partial result. The cap does not
include the read-only loaded table; use `--memory-limit-mb` for table
admission.

## Out of scope

DDL/DML, transactions, recursive CTEs, stored procedures, user-defined
functions, locale-aware collations, named time zones, arbitrary-precision
decimals, durable spill and recovery, distributed execution, and database wire
protocols are not implemented.
