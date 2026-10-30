# Release Notes

Behaviour changes worth reading before an upgrade. The GitHub release page
lists every merged change.

## Unreleased

### TraceQL: structural operators and the Tempo pipeline

TraceQL now accepts Tempo's structural grammar and evaluates it against the
real span tree, in search, metrics and autocomplete alike. See
[TraceQL](traceql.md) for the full semantics.

#### New

- All 15 structural operators: `>`, `>>`, `<`, `<<`, `~`, their `!` variants
  (`!>`, `!>>`, `!<`, `!<<`, `!~`) and their `&` variants (`&>`, `&>>`, `&<`,
  `&<<`, `&~`).
- Pipelines: scalar filters (`count`, `avg`, `min`, `max`, `sum`) over the
  whole preceding expression, `by()`, `coalesce()`, `select()`, stages at any
  position with narrowing (`{A} | {B} > {C}`), and parenthesised pipelines as
  operands.
- Field expressions in spanset filters and aggregate arguments: attribute
  vs attribute (`{ .a = .b }`), arithmetic (`+ - * / % ^`), unary `-` and `!`,
  and `= nil` / `!= nil`.
- Search spansets carry aggregate values and `by()` keys as spanset
  attributes; `by()` groups are returned as separate spansets.
- The `spss` search parameter.
- The `sample` and `exemplars` hints for TraceQL metrics.
- Tag and value autocomplete suggest only from the spans the query returns,
  and accept queries with `&&`, `||` and structural operators.
- The nested-set intrinsics `nestedSetLeft`, `nestedSetRight` and
  `nestedSetParent`, as ints, in filters, field expressions, `select()`,
  `by()`, aggregates and metrics, returned as `intValue`. An opt-in
  background numberer computes them; see
  [Nested-set intrinsics](traceql.md#nested-set-intrinsics). Grafana's Traces
  Drilldown "Service structure" tab works.
- Every search response echoes the fields of the query's spanset filters
  onto the listed spans, as if they were selected with `select()`.

#### Results that change

These queries returned wrong results before and now return exact ones.
Saved dashboards and alerts that use them will see different numbers.

- **`&>>`, `!>>`, `~`** were evaluated as `&&` and never looked at the span
  tree. They now evaluate parent, ancestor and sibling relations.
- **`&&` between selectors** matched only when a single span satisfied both
  sides and both sides' newest timestamps were equal. `{A} && {B}` now
  matches a trace when both sides have spans in it and returns the spans of
  both sides. **`||`** returns the spans of either side, each once. Chains of
  three or more operands no longer drop operands.
- **Metrics** flattened every selector's filters into one AND list, ignoring
  `||` and structural operators, so `({A} >> {B}) | rate()` counted spans
  matching A and B at once. A metrics query now counts exactly the spans a
  search for the same expression returns.
- **Scalar filters** (`| count() > 2`) applied to the last selector only.
  They now apply to the whole preceding expression's result spans, and a
  scalar filter before a metrics function decides which traces contribute.
- **Span cap.** Each selector was capped at 100 spans before operators were
  evaluated, which dropped true matches in large traces. Operands are now
  uncapped. Search lists **3 spans per spanset by default** (was up to 100);
  set `spss` for more. `matched` reports the true number of matched spans.
- **`durationNanos`** is always the span duration in nanoseconds. A span
  whose duration equaled its start timestamp reported `"n/a"`, which Grafana
  failed to parse.
- **Time range.** TraceQL search, metrics and autocomplete with a filtering
  query now count spans that start in **`(from, to]`**: a span starting
  exactly at `from` is excluded, one starting exactly at `to` is included.
  Search and metrics previously used `[from, to)`.
- **`metrics by (...)` labels.** `by (span.x, name)` could attach values to
  the wrong label names; labels now match their values.
- **Filters mixing intrinsics and attributes.** Filters combining `status`,
  `kind`, `rootName`, `rootServiceName` or `duration` with attribute terms
  (for example `{ status = error && .a = 1 }` or
  `{ duration > 1s || .a = 1 }`) could miss matching spans; they now return
  every match.
- **`&&` and `||` inside `{...}`** are now one left-associative level, as in
  Tempo. `{ .a = 1 && .b = 2 || .c = 3 }` was read as
  `.a = 1 && (.b = 2 || .c = 3)`; it is now `(.a = 1 && .b = 2) || .c = 3`.
  Add parentheses to keep the old reading.
- **`{ .a = .b }`** compares two attributes; `.b` used to be the string
  `".b"`.
- **`({A} | count() > 1)`** now returns the `count()` spanset attribute.

#### Queries that now fail

- `<<&` and `<<~` are parse errors naming Tempo's spelling (`&<<`, `!<<`).
- Two spansets with no operator between them (`{A} {B}`) and a trailing
  operator (`{A} &&`) are parse errors.
- `status` and `kind` compare only against their own keywords:
  `status = "error"` is a type error, write `status = error`.
- A structural query on ClickHouse older than 24.9 fails with
  `TraceQL structural operators need ClickHouse 24.9 or newer`.
- A structural query meeting a trace deeper than the round limit, or whose
  parent links form a cycle, fails instead of returning partial results.

#### Other changes

- Clustered deployments: TraceQL search reads the Distributed tables, so it
  sees traces on every shard. It used to read only the shard-local tables of
  the node it ran on.
- TraceQL search reads the whole (limit-bounded) result before responding, so
  errors raised during the query reach the client.
- `{}` can be an operand of `&&`, `||` and structural operators.
- Each search spanset lists its **earliest-starting** spans, earliest first
  (ties by span id), where it listed the newest. A plain search shows the
  first spans of each trace instead of the last.
- Value autocomplete (`/api/v2/search/tag/{tag}/values`) accepts scoped tags
  (`span.x`, `resource.x`, `.x`), as Grafana sends them, and without a query
  lists the tag's values. Tag autocomplete accepts a lone `{}` query.
- Autocomplete for costly queries stays filtered by the query instead of
  falling back to every tag.

#### For operators

- **ClickHouse 24.9 or newer** is required for structural queries. Other
  TraceQL queries are unaffected. Structural queries run with
  `enable_analyzer=1`.
- **Schema migration.** The upgrade creates the `tempo_traces_nested_set`,
  `tempo_traces_touched`, `tempo_traces_numbered`,
  `tempo_traces_nested_set_batch` and `tempo_traces_nested_set_lease`
  tables, the `tempo_traces_touched_mv` materialized view on
  `tempo_traces`, the `tempo_traces_numbered_mv` materialized view on
  `tempo_traces_nested_set` and, on clusters, the Distributed
  `tempo_traces_nested_set_dist`, `tempo_traces_nested_set_batch_dist` and
  `tempo_traces_nested_set_lease_dist`, plus a Distributed read-path table
  over the read cluster when one is configured (see
  [Cross-Cluster Deployment](configuration.md#cross-cluster-deployment)).
  The local tables' engines follow `tempo_traces`, except
  `tempo_traces_nested_set_batch`, a plain MergeTree on every node.
  `tempo_traces_touched` is ordered by `(toStartOfMinute(inserted_at),
  trace_id)` and `tempo_traces_numbered` by `(intDiv(version, 60000),
  trace_id)`.
  Structural queries alone need no migration, writer change or backfill.
- **New nested-set numberer.** It runs on every `all` or `writer`
  instance, which elect one runner through ClickHouse; there is nothing to
  enable. `GIGAPIPE_NESTED_SET_NUMBERER_IDLE` (10s), `_INTERVAL` (5s) and
  `_MAX_SPANS` (200000, per shard on clusters) tune it. If the runner stops,
  another instance takes over within 4 × `_INTERVAL`. It needs ClickHouse
  24.9 or newer; on an older server it doesn't start and every span reads
  `0`/`0`/`0`. There is no backfill: traces stored more than 5 minutes
  before its first successful pass may never be numbered. See
  [TraceQL](traceql.md#the-numberer) and
  [Configuration](configuration.md#traceql).
- **New setting `GIGAPIPE_TRACEQL_ANCESTRY_ROUNDS`** (1 to 32, default 20).
  Traces up to `2^R` spans deep resolve. See
  [Configuration](configuration.md#traceql).
- **Planner plugins.**
  - A `TracesDataPlugin` receives rows of `trace_id`, `span_id` (all listed
    span ids) and `spansets` (an array of group key, span ids, `matched` and
    attributes), and must emit 16 columns: the 8 existing ones, then
    `spanset_span_ids`, `matched`, `attr_names`, `attr_types`, `attr_values`,
    `attr_texts` (one element per spanset), `span_attr_keys` and
    `span_attr_vals` (one element per listed span). Plugins don't receive the
    `select()` or echoed filter fields, so neither returns values through a
    plugin.
  - An `InitIndexPlannerPlugin`'s select must expose unqualified `trace_id`
    and `span_id` columns.
- **Query cost.** Structural operators read the parent links of their
  candidate traces from the traces table with no time bound, so every daily
  partition in retention is looked up. `>>` and `<<` then run every configured
  round.

#### Deliberate differences from Tempo

gigapipe returns what Tempo returns except for these, also listed in
[TraceQL](traceql.md#differences-from-tempo).

**Time range and trace data**

- Operands must start in the query range. Tempo draws operands from every
  span of a trace that overlaps the range.
- Tempo's rule that returns nothing for a trace in which three or more spans
  share a span ID is not reproduced; such traces are evaluated as stored.

**Grouping**

- The grouped-spanset error is raised before the query runs, so it also
  fires on queries Tempo would run because every trace has a single group,
  and on spanset operators after `by()` (`{A} | by(.x) | {B} > {C}`), which
  Tempo evaluates per group.
- Groups are ordered by key; Tempo's order is random.
- Attribute group keys are returned as strings, since attributes are stored
  untyped; Tempo returns typed values. `status` and `kind` read the stored
  attribute, so spans without one form the nil group, where Tempo reports
  `unset`.

**Field expressions and filters**

- `!` works inside spanset filters; Tempo returns nothing for such filters.
- `!x` on a non-boolean is false, where Tempo can fail.
- `span.a = nil`, `resource.a = nil` and `.a = nil` all test `a` regardless
  of scope, and match only spans without `a`. In Tempo the scope matters, and
  unscoped `.a = nil` also matches spans that have `.a`.
- Int, float and duration values aren't distinguished in storage, so
  `.a = "200"` and `.a = 200` both match a stored `200`.
- `int ^ int` is evaluated as written; Tempo swaps base and exponent.
- Integer division and modulo by zero are nil, not an error.
- `&&` and `||` treat a nil operand as false and evaluate the other side;
  Tempo usually makes the whole operation false.
- Typing is stricter: a filter that can't be boolean or an aggregate argument
  that can't be numeric, built only from attributes (`{ -.a }`,
  `{ .a * .b }`, `{ !.a = 1 }`, `{ .a + .b = "x" }`, `avg(!.a)`), is
  rejected before the query runs. Tempo accepts these and matches nothing or
  fails per span.

**Scalar filters**

- In aggregate arguments, a span where the expression is nil (`.a + nil`) is
  skipped; Tempo counts it.
- `sum()` over a mix of ints and floats adds every value as a float; Tempo
  drops values whose type differs from the first value's.
- Literal arithmetic isn't folded: `count() > 1 + 2` is rejected.
- `count() > count()` is rejected with Tempo's right-hand-side error text.
- `(count() > 2) && {B}` is a parse error; Tempo accepts it.
- A bare word as an aggregate argument (`avg(spanN)`) reads the attribute of
  that name; Tempo rejects it.

**Nested-set intrinsics**

- Nested-set siblings are numbered by start time, then span id; Tempo
  numbers them in its storage order, so the absolute values can differ.
- Spans sharing a span id form one nested-set node, placed by the
  earliest-starting copy; Tempo matches two copies by kind and zeroes a trace
  in which three or more share an id.
- Every pass renumbers whole traces, so merges leave no stale nested-set
  numbers, as they can in Tempo.
- A trace reads `0`/`0`/`0` until the numberer has numbered it, about 15s
  after its last span with the defaults, and traces stored before the
  numberer ran may never be numbered.

**Search results**

- Each spanset lists its `spss` earliest-starting spans; Tempo lists the
  first spans in its storage order. Under clock skew, a child can be kept
  ahead of its parent.
- The filter echo uses the union of the query's keys:
  `{ .a = 1 } >> { .b = 2 }` shows `.a` on returned spans that carry it,
  where Tempo shows only `.b`.
- Fields read only by a scalar filter or `by()` aren't echoed; Tempo
  returns them too.

**`select()`**

- Selected attribute values are returned as strings; Tempo returns typed
  values.
- An attribute named like an intrinsic shares its storage key: a span
  attribute `name`, `status` or `kind` is reported as the span's name,
  status or kind.

**Metrics hints**

- `sample` is a hash of the trace or span id; Tempo keeps one item in N with
  a counter.
- Sampled metrics values are returned unscaled; Tempo scales them up by the
  observed sampling ratio (except `min_over_time` and `max_over_time`).
- `exemplars=N` caps exemplars per series, spread over the buckets that have
  one; Tempo applies the cap per query evaluator, not per series.
