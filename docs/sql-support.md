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
scan-side conjunct pushdown in safe inner/cross-join query blocks, transitive
predicates across inner equi-joins, integer range contradiction elimination,
projection pruning, cheap-first short-circuit filter ordering, Top-K
replacement, selectivity-aware safe inner-join reordering, hash-join selection,
build-side selection, and hash-probe runtime filters. Predicates that cannot be
pushed safely remain as residual filters above joins.

Set `DREMEL_DISABLE_OPTIMIZER=1` to build a comparable unoptimized plan.
`EXPLAIN` shows physical operators, estimates, join choices, and applied rules.

## Parquet execution

`--direct-parquet` keeps only Parquet file metadata resident at startup. Each
query requests its referenced event columns from the official Rust or C++
Apache Parquet reader and prunes row groups with exact min/max/null statistics
for supported comparisons, positive `BETWEEN`, positive `IN`, `IS NULL`, and
boolean conjunctions. Unsupported expressions conservatively retain the row
group. `EXPLAIN` reports selected columns, row groups, rows, and compressed
column-chunk bytes. The byte counter is the sum of selected compressed chunks
from file metadata, not an operating-system I/O counter. Query `--stats`
reports the same scan counters.

The selected row groups are decoded into the existing in-memory operators.
Dimension tables used by joins retain the existing load path.

`--streaming-parquet` uses the same projection and pruning plan but decodes at
most `--batch-size` rows through the official Arrow record-batch reader before
passing the batch to a scan or aggregate operator. Dictionary identifiers stay
stable between batches, and final ordering, distinctness, limits, and offsets
are applied across the complete result. Metadata-only `COUNT(*)` decodes no
data batches. Query stats include batches read, peak decoded batch bytes, and
the fallback state.

Inner and left joins from `events` to `users.user_id` or
`campaigns.campaign_id` stream fact batches through a cached primary-key index.
Nonaggregate joins support global ordering, Top-K, offsets, and distinctness.
Grouped join results merge `COUNT`, `SUM`, `MIN`, and `MAX` states across
batches. Right, full, cross, and non-key joins, join queries using `AVG`,
windows, CTEs, subqueries, `UNION`, and `HAVING` fall back to the materialized
direct path so they preserve the full SQL behavior. Direct and streaming flags
are mutually exclusive. Async scheduler submissions do not yet use either
Parquet query mode.

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
is monotonic for ordinary operators. Spill operators use owned reservations
that are released after each partition, while peak usage remains recorded. The
value is an operator accounting metric, not process RSS. Budget exhaustion returns
`RESOURCE_EXHAUSTED` without returning a partial result. The cap does not
include the read-only loaded table; use `--memory-limit-mb` for table
admission.

`--spill-dir` enables automatic disk partitioning for eligible event-table hash
aggregations when their estimated group state exceeds half of the available
query budget. Spill execution currently requires `GROUP BY`, `ORDER BY`, and
`LIMIT`, with at most three group keys, on a single `events` table query without
`HAVING`, CTEs, unions, windows, or subqueries. It supports the same `COUNT`,
`SUM`, `AVG`, `MIN`, and `MAX` states as in-memory aggregation. The input is
hash partitioned by group key, each partition is aggregated within a fixed
share of the hard query cap, and a bounded global Top-K is retained across
partitions.

Spill files contain internal row references and are not a persistent storage
format. Each query creates a uniquely named workspace below the configured
directory. The workspace is removed on normal completion, memory failure,
execution error, or cancellation. Query stats expose current and peak accounted
memory, partition and file counts, bytes written/read, passes, and whether the
operator spilled. Queries outside this shape retain the existing
`RESOURCE_EXHAUSTED` behavior.

## Out of scope

DDL/DML, transactions, recursive CTEs, stored procedures, user-defined
functions, locale-aware collations, named time zones, arbitrary-precision
decimals, Parquet page-index pruning, external merge sort, durable spill
recovery, distributed execution, and database wire protocols are not
implemented.
