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

## Named-table SQL

The Rust CLI accepts `--table NAME` with an Arrow IPC or Parquet file. The
file schema supplies field names, types, and nullability. Table and column
identifiers are case insensitive; names that differ only by case are rejected.
Quoted identifiers are not supported.

This path supports projection (including `SELECT *`), filters, scalar
expressions, `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, grouping, HAVING, DISTINCT,
ordering, LIMIT, and OFFSET. ORDER BY must identify one projected column or
alias. HAVING may reference grouped columns and aggregates. Unsupported
functions, types, and query shapes produce errors.

Supported inputs are null, boolean, signed and unsigned integers that fit in
INT64, FLOAT32/FLOAT64, UTF8/LARGEUTF8/UTF8VIEW, primitive dictionaries,
DECIMAL128 with scale 2, DATE32/DATE64, and timestamps with whole-second values.
Decimal values must fit the engine's DECIMAL(18,2) range. DATE64 values must
represent whole days. Subsecond timestamps and out-of-range numeric values
are rejected rather than truncated. STRUCT fields and LIST containers are also
supported; see [nested SQL](#nested-sql) for path and value semantics.

Execution is serial and scans typed Arrow batches without first converting
the input into scalar rows. The file is loaded eagerly; ordinary table joins, windows, CTEs,
subqueries, UNION, spill, and direct/streaming Parquet flags are unsupported
in this mode. `--memory-limit-mb` checks resident input after loading, while
`--query-memory-limit-mb` accounts for column normalization, aggregation,
distinctness, and result buffers. These are estimates, not a hard process RSS
limit. A result exceeding `--max-result-rows` fails before any rows are printed.

The Rust API supports preparation and repeated execution against a matching
schema:

```rust
use dremel::{ColumnarQuery, ColumnarTable};

let table = ColumnarTable::read("measurements.parquet")?;
let query = ColumnarQuery::prepare(
    "measurements",
    table.schema().clone(),
    "SELECT region, COUNT(*) AS n FROM measurements GROUP BY region ORDER BY region",
)?;
let result = query.execute_with_memory_limit(&table, 64)?;
result.write_ndjson(&mut std::io::stdout())?;
```

Results include an Arrow schema and canonical scalar rows. Dates use ISO date
strings, timestamps use epoch seconds, and decimals use scaled integers inside
the scalar API. `write_ndjson` uses the same typed JSON format as the CLI.
Changing the field layout requires preparing the query again. Caller-owned
input buffers are excluded from the API's query workspace budget.

The schema benchmark runs renamed fields through Arrow and Parquet, checks
results against SQLite and both built-in engines, and generates an interactive
report. Preparation, startup loading, and prepared execution are separate:

```bash
.venv/bin/python benchmarks/scripts/run_schema_benchmark.py --warmup 3 --iterations 20
```

The full benchmark runner enables this suite by default. Set `SCHEMA_SQL=0`
to disable it. The built-in controls use one thread and the same logical
values; their specialized execution paths differ from the named-table path.

## Nested SQL

Rust named-table queries resolve arbitrary-depth STRUCT paths. Paths may be
unqualified (`profile.detail.zip`), table-qualified
(`records.profile.detail.zip`), or alias-qualified
(`r.profile.detail.zip`). Field names are case insensitive. If a path could
mean both a table qualification and an unqualified STRUCT path, preparation
rejects it as ambiguous; change the alias or qualify it with the table name.
Unknown children and duplicate sibling names differing only by case are
rejected. Quoted identifiers and escaping dots within field names are not
implemented.

```bash
./target/release/dremel query --data records.parquet --table records \
  --sql "SELECT r.profile.city, r.profile.detail.zip, CARDINALITY(r.samples) FROM records r"
```

A null ancestor makes the field path null, even if its child buffer contains
a value. Primitive leaves support the existing scalar expressions, filters,
aggregates, grouping, ordering, and DISTINCT. Selecting one field evaluates
only that field, not its siblings; input files are still loaded eagerly.
This does not imply SQL Parquet projection, pruning, or streaming.

Whole STRUCT, LIST, LARGE_LIST, and FIXED_SIZE_LIST values can be projected,
including nested lists and lists of structs. Their leaves use the same
primitive types and lossless normalization rules as flat SQL. Unsupported
leaf types, MAP, UNION, run-end encoding, list-view arrays, and nested
dictionary values are rejected when referenced. Primitive dictionaries are
decoded by Apache Arrow. A null struct is distinct from a valid struct whose
fields are all null. Null lists, empty lists, null elements, and element order
remain distinct. Values hidden beneath null parents or outside a sliced list's
visible offsets are not treated as visible values during normalization.

`IS NULL` tests the outer container validity, not whether all its children are
null. `COUNT(container)` counts non-null containers, including empty lists.
`CARDINALITY(list)` returns its element count, including null elements; it
returns null for a null list. `CASE` and `COALESCE` accept container values
only when non-null branches have identical normalized Arrow field layouts.
Containers support structural comparisons, IN/NOT IN, BETWEEN, NULLIF,
MIN/MAX, DISTINCT, grouping, HAVING, and explicit sort keys. Container casts,
arithmetic, SUM/AVG, and string functions remain unsupported.

The Rust `Scalar` API adds `Struct(Vec<(String, Scalar)>)` and
`List(Vec<Scalar>)`. The result schema retains container shapes and field names,
with normalized primitive leaf types. Typed NDJSON is recursive:

```json
[{"t":"struct","v":{"city":{"t":"s","v":"NY"}}},{"t":"list","v":[{"t":"i","v":1},null]}]
```

The `struct` payload is an object with original Arrow field names; a `list`
payload is an ordered array. Nulls remain JSON null at every depth. As with
flat queries, date values use ISO strings and timestamps use epoch seconds.
Query workspace accounting includes normalized leaf buffers, combined ancestor
validity, list visibility workspace, and recursive result allocations. It is
an estimate, not a hard allocation or RSS limit; temporary expression values
are not comprehensively budgeted. This path remains serial and materialized.

Nested SQL does not implicitly traverse a list or flatten records.
`items.price` is valid as a storage projection but is not a scalar SQL path;
use explicit [UNNEST](#unnest) for repeated fields. The C++ reference still
supports only the fixed-schema SQL workloads.

```bash
.venv/bin/python benchmarks/scripts/run_nested_sql_benchmark.py --warmup 3 --iterations 20
```

The suite validates 18 queries over Arrow and Parquet against independent
SQLite null-aware scalar paths and PyArrow typed values. Prepared execution
includes normalization and result materialization; loading, preparation, and
JSON encoding are outside the timed interval. Eligible queries also run over
equivalent flat Rust values, with ancestor nulls already applied and matching
input formats, batch sizes, row groups, and primitive widths. Container
queries have no flat timing control. These measurements are not a Rust/C++
language comparison. The suite is enabled by default by the full runner;
set `NESTED_SQL=0` to disable it. Results include an interactive HTML report.

### Structural value operators

STRUCT values compare in schema field order; lists compare lexicographically
by element. The first differing child determines ordering, and a shorter list
sorts before a longer list with the same prefix. Comparison uses typed values,
not JSON text: `[2]` sorts before `[10]`. Container field names and normalized
leaf types must match for comparison; field name case and nullability do not
change values. Primitive widths and list offset widths normalize without
changing semantics. No implicit numeric widening occurs inside containers.

An outer NULL operand makes an ordinary comparison unknown, just as for a
scalar. Inside a valid container, null children are structural values: two
null children compare equal, and a null child sorts before a non-null child.
This inner ordering is fixed and does not change with the outer ORDER BY
NULLS FIRST/LAST setting. A null list, `[]`, and `[NULL]` remain distinct;
a null struct remains distinct from a valid all-null struct.

Grouping and DISTINCT use recursive typed keys, not serialized JSON. Floating
children canonicalize signed zero and NaN payloads; all NaNs compare equal
and sort after positive infinity inside containers. MIN/MAX skip outer NULLs,
retain the complete typed winning value, and use the same structural order.
These are the engine's documented container semantics, not a claim that every
SQL database orders nested values identically.

```sql
SELECT profile, COUNT(*) AS n, MIN(samples), MAX(samples)
FROM records
GROUP BY profile
HAVING profile = profile AND COUNT(*) > 1
ORDER BY profile NULLS LAST;
```

The same comparisons work in correlated UNNEST ON predicates, including
joins between STRUCT or LIST elements. Null outer elements do not match with
`=`, while valid containers containing null children can match. LEFT JOIN
preservation still happens before WHERE filtering. Independent table joins
remain unsupported without a catalog.

Operators read only referenced paths; the engine does not flatten the whole
parent record. Evaluating a referenced container constructs its typed value,
and grouping, extrema, sorting, and output may retain those values. Query
workspace estimates include recursive keys, retained extrema, DISTINCT keys,
and sort workspace; transient expression allocations are not fully bounded.
EXPLAIN reports `NestedValueExec(keys=typed;order=lexicographic;...)` for
container-valued access. Execution remains eager, serial, and materialized.

```bash
.venv/bin/python benchmarks/scripts/run_nested_operators_benchmark.py --warmup 3 --iterations 20
```

The suite checks 22 queries in both Arrow and Parquet formats against an
independent structural oracle over official PyArrow values. Selected scalar
results also use SQLite and prepared Rust controls over precomputed,
order-preserving integer ranks. Rank encoding and loading are outside timing;
this isolates nested evaluation overhead, not end-to-end flattening costs or
a Rust/C++ winner. Both formats use 4096-row batches/row groups. The runner
enables this suite by default; set `NESTED_OPERATORS=0` to disable it. JSON
results and an interactive HTML report are written under
`results/nested-operators/`.

## UNNEST

Rust named-table queries expand LIST, LARGE_LIST, and FIXED_SIZE_LIST fields
through correlated `CROSS JOIN`, comma joins, `INNER JOIN ... ON`, and
`LEFT JOIN ... ON`. The argument must be one field path from the input table
or an earlier UNNEST relation. `LATERAL` is optional. A relation alias is
required; its default element column is `value`, and `WITH ORDINALITY` adds
`ordinality`. Explicit column aliases use `AS u(element)` or
`AS u(element, position)` with ordinality.

```sql
SELECT r.record_id, item.value.price, tag.value, tag.ordinality
FROM records r
CROSS JOIN UNNEST(r.items) AS item
LEFT JOIN UNNEST(item.value.tags) WITH ORDINALITY AS tag ON TRUE
ORDER BY record_id, ordinality
```

Each parent list is visited in storage order. Ordinality starts at one for
each parent, includes null elements, and is assigned before ON or WHERE
filtering. Use explicit ORDER BY on projected parent keys and ordinalities
when final result order matters. Null lists and empty lists produce zero
rows in cross/inner joins. A null element produces one row with a null
element and a real ordinality. A left join produces one null-extended row
with null ordinality if there are no elements satisfying ON, including
null and empty lists. WHERE runs after join preservation and may remove it.
These join and ordinality rules follow the conventional
[LATERAL table-function model](https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-TABLEFUNCTIONS).

Elements can be primitives, whole typed STRUCT/LIST values, STRUCT leaf
paths such as `u.value.price`, or sources for a later UNNEST. Separate lists
form a Cartesian product, not a positional zip. Chained lists preserve
their own parent and ordinal domains. SELECT * includes the parent fields,
each element column, and each requested ordinality. Ordinary name binding
and ambiguity checks apply; use qualified columns when names repeat.
ON cannot reference a later stage, contain aggregates, or use a non-boolean
expression. RIGHT/FULL joins, multiple arguments in one UNNEST, array
indexing, arbitrary list expressions, standalone FROM UNNEST, and more than
32 stages are unsupported.

Scalar filters, aggregates, grouping, HAVING, DISTINCT, ordering, LIMIT,
and OFFSET work over expanded primitive values. Existing container-operation
restrictions still apply. COUNT(*) counts expanded rows, not source records.
Execution retains Arrow arrays and a cursor of parent/element indices; it
does not build an intermediate copy of each parent record. Only requested
leaf arrays are normalized; unrelated element siblings are not evaluated.
Null ancestor and sliced-list visibility masks apply before normalization.
EXPLAIN shows `CorrelatedUnnestExec` with `parent_copy=false`.
Named-table `--stats` counts distinct top-level input fields accessed by
execution in `columns_scanned`, including list sources but not generated
ordinalities. It is not a Parquet decoding or selected-byte metric: loading
still reads the entire file.

Input remains eager, execution serial, and results materialized. Cursor,
visibility, normalization, group, and result allocations are estimated in
the query budget; this is not a hard allocator/RSS limit. The input load is
excluded from that budget, and no SQL Parquet projection or streaming is
implied.

```bash
.venv/bin/python benchmarks/scripts/run_unnest_benchmark.py --warmup 3 --iterations 20
```

The suite checks 22 queries in both Arrow and Parquet against independent
PyArrow row expansion and SQLite, including typed container elements.
It measures prepared execution and compares eligible queries with Rust over
equivalent pre-expanded scalar rows. Expansion and loading are outside flat
control timing, so ratios do not measure end-to-end flattening cost. Flat
row counts differ from input record counts, and count-only controls can use
metadata while UNNEST visits offsets. C++ does not implement generic UNNEST;
this is not a language comparison. Both standalone and full benchmark runs
generate an interactive HTML report. Set `UNNEST=0` to skip this suite.

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
Grouped join results merge `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` states across
batches. For `AVG`, each batch supplies its non-null count so partial averages
are weighted correctly. Right, full, cross, and non-key joins,
windows, CTEs, subqueries, `UNION`, and `HAVING` fall back to the materialized
direct path so they preserve the full SQL behavior. Direct and streaming flags
are mutually exclusive. Async scheduler submissions support both modes with
the same prepared plans and result semantics as synchronous execution.

## Concurrent execution

The long-lived benchmark server has a bounded admission queue, global and
per-group memory limits, active-query limits, deadlines, cancellation, and a
4:2:1 priority-weighted scheduler. FIFO order is preserved within each
priority/resource group. Each admitted request's memory reservation is also
its hard query workspace cap. Direct and streaming Parquet requests use this
cap for query operators; `--memory-limit-mb` separately bounds decoded event
tables or batches. Cancellation is checked between decoded batches. The
concurrency benchmark reports throughput, outcomes, queue and execution latency
percentiles, and Jain's fairness index.

## Memory-bounded execution

`--query-memory-limit-mb` applies a query-scoped budget shared by all worker
threads. Both engines account scan selections, hash aggregation
tables, join build tables and outputs, distinct sets, window state,
intermediate relations, top-k buffers, and result materialization. Accounting
is monotonic for ordinary materializing operators. Spill and result-streaming
operators use owned reservations that are released after each partition,
batch, or row, while peak usage remains recorded. The value is an operator
accounting metric, not process RSS. Materialized-query budget exhaustion
returns `RESOURCE_EXHAUSTED` without returning a partial result. The cap does
not include the read-only loaded table; use `--memory-limit-mb` for table
admission.

`--stream-results` writes one typed-NDJSON array per row without materializing
the complete result. It supports scalar projection and filtering over a single
`events` scan, plus `LIMIT` and `OFFSET`. `DISTINCT`, aggregation, joins,
windows, CTEs, unions, and subqueries are rejected with
`STREAMING_UNSUPPORTED`. The same sink accepts official Arrow-backed record
batches when combined with `--streaming-parquet`.

For native DREMCOL1 and `--streaming-parquet` scans, `ORDER BY` is supported when result streaming is
combined with `--spill-dir` and a nonzero `--query-memory-limit-mb`. The
external sort fills in-memory runs up to half of the available query budget,
writes each run in a private binary workspace, reduces large run sets through
bounded fan-in merge passes, and heap-merges the final runs into the result
sink. Ordering direction, explicit or default null placement, deterministic
output ties, `LIMIT`, and `OFFSET` match the materialized path. Parquet scans
retain projected-column decoding and row-group pruning. `--memory-limit-mb`
caps each decoded Parquet batch separately from the sort's query memory cap.
`--direct-parquet` cannot be combined with ordered result streaming.

The streaming path accounts the scan-selection batch, current result row,
external-sort run and merge buffers when used, and output encoder buffer. Stats
expose emitted rows, output bytes, result batches, spill I/O, and current and
peak query memory. An output or execution failure can happen after earlier rows
have been written, so a consumer must require a successful process exit before
accepting the stream as complete.

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
decimals, Parquet page-index pruning, durable spill
recovery, distributed execution, and database wire protocols are not
implemented.
