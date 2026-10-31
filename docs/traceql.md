# TraceQL

gigapipe implements the Tempo query API for TraceQL search, TraceQL metrics
and tag/value autocomplete:

- `GET /api/search` - TraceQL search (`q`, `start`, `end`, `limit`, `spss`)
- `GET /api/metrics/query_range`, `GET /api/metrics/query` - TraceQL metrics
- `GET /api/v2/search/tags`, `GET /api/v2/search/tag/{tag}/values` - autocomplete (`q`)

Search and metrics are also served under the `/tempo` prefix.

This page covers the parts of TraceQL that relate spans to each other
(structural operators, `&&`/`||` between spansets, pipelines, the nested-set
intrinsics) and the field expressions used inside spanset filters. Where
gigapipe deliberately differs from Tempo, the difference is listed in
[Differences from Tempo](#differences-from-tempo).

## Requirements

Queries that use a structural operator (`>`, `>>`, `<`, `<<`, `~` and their
`!`/`&` variants) need **ClickHouse 24.9 or newer**. On an older server they
fail with `TraceQL structural operators need ClickHouse 24.9 or newer`. All
other TraceQL queries keep working on older servers.

Structural queries need no schema migration, writer change or backfill: the
span tree is resolved at query time from the `parent_id` of the stored spans.
Clustered (Distributed) deployments return the same results as single-node
ones, provided **a trace's spans and attribute rows live on one shard**.
Writers inserting through the `_dist` tables place them so: the sharding key is
`sipHash64(oid, trace_id)`, and a trace has one `oid`. A structural query
(search, metrics, `compare()`, tag autocomplete) resolves relations on each
shard over that shard's spans; a split trace loses the relations between its
shards' spans. Search keeps the newest traces of the shards' results and can
list a split trace once per shard. `compare()`, structural or not, counts
attribute values on each shard over that shard's traces, and the reader sums
the shards' counts. The nested-set numberer has the same requirement.
For the ClickHouse memory setting and per-node sizing of 24h structural
queries, see [ClickHouse memory](configuration.md#clickhouse-memory).

The nested-set intrinsics read values that a background numberer stores,
which also needs ClickHouse 24.9 or newer; see
[Nested-set intrinsics](#nested-set-intrinsics).

## Time range

A span is in range when it **starts** in `(from, to]`: a span starting exactly
at `from` is excluded, one starting exactly at `to` is included. The rule is
the same for search, metrics and autocomplete with a filtering query, so
adjacent windows never count a span twice. Autocomplete without a query, or
with `{}`, reads whole days.

Only spans in range can be operands or results. The structural context of a
span — its parent, ancestors, siblings and roots — is read from the whole
trace, whatever time those spans started. A long-running root that started
before `from` still links its descendants inside the range.

## Structural operators

Each operator relates the spans of its left-hand side (LHS) to those of its
right-hand side (RHS) within one trace.

| Operator | Returns |
|----------|---------|
| `{A} > {B}` | B spans whose parent matches A |
| `{A} >> {B}` | B spans with any ancestor matching A |
| `{A} < {B}` | B spans that are the parent of a span matching A |
| `{A} << {B}` | B spans that are an ancestor of a span matching A |
| `{A} ~ {B}` | B spans that share a parent with a different span matching A |

Every operator has two variants:

- **`!` variants** (`!>`, `!>>`, `!<`, `!<<`, `!~`) return the RHS spans for
  which the positive relation finds no match. When the LHS matches nothing,
  every RHS span is returned.
- **`&` variants** (`&>`, `&>>`, `&<`, `&<<`, `&~`) return the matching RHS
  spans and the LHS spans they matched, each span once.

Inside `{...}`, `>`, `<` and `!~` keep their meaning as comparison and regex
operators. Between two spansets they are structural, so
`{ .x > 3 } > { .y !~ "a.*" }` parses as expected. Operators must be written
without spaces: `! >` and `> >` are parse errors. `<<&` and `<<~` are not
TraceQL operators and fail with a parse error naming the spelling to use
(`&<<` and `!<<`).

### Roots, orphans and incomplete traces

- A span takes part in a structural relation only when its parent chain
  reaches a root span (a span without a parent).
- Spans that can't reach a root — an orphan subtree whose parent was never
  ingested, or every span of a trace with no root — never match a positive or
  `&` operator, and always match a `!` operator.
- A span whose ancestor hasn't been ingested yet starts matching once that
  ancestor lands. No action is needed.
- When a trace has several roots, all of them are siblings under `~`.

### Depth limit

Ancestry is resolved in up to `R` rounds, set by
`GIGAPIPE_TRACEQL_ANCESTRY_ROUNDS` (see [Configuration](configuration.md#traceql)).
Each trace takes one round per doubling of its spans found so far.
With `R` rounds, traces up to `2^R` spans deep resolve; the default of 20
covers traces about a million spans deep.

A query that meets a trace deeper than that, or a trace whose parent links
form a cycle, fails instead of returning partial results:

```
TraceQL structural query: a trace is deeper than the round limit of 20 rounds (1048576 spans) or its parent links form a cycle; raise GIGAPIPE_TRACEQL_ANCESTRY_ROUNDS
```

The check covers every trace that has spans on both sides of a `>>`/`<<`
operator (or its variants). One-hop operators (`>`, `<`, `~` and their
variants) resolve ancestry, and run the check, only on traces that have a root
and also a span whose parent is missing; on other traces they never fail.

## `&&` and `||` between spansets

- `{A} && {B}` matches a trace when both sides have spans in it, and returns
  the spans of both sides.
- `{A} || {B}` returns the spans of either side.
- A span matching both sides is returned once.

`{}` matches every span in range and can be used as an operand, for example
`{} && { .a = 1 }`.

## Precedence

Between spansets, from loosest to tightest:

1. `|` (pipeline stages)
2. `&&` and `||`, at the same level
3. the 15 structural operators

All operators are left-associative. So `{A} || {B} && {C}` is
`({A} || {B}) && {C}`, and `{A} > {B} > {C}` is `({A} > {B}) > {C}`: C spans
whose parent is a B span whose parent is an A span. Parentheses change the
grouping: `{A} > ({B} > {C})` returns C spans whose parent matches both A and
B.

Inside `{...}`, see [Field expressions](#field-expressions).

## Pipelines

A query is a pipeline of elements separated by `|`. An element is a spanset
expression, a scalar filter, `by()`, `coalesce()` or `select()`. A metrics
function, `with(...)` hints and a second stage (`topk`/`bottomk`) can only
come last.

A parenthesised pipeline can be used as an operand:

```
({ .service = "api" } | count() > 2) >> { status = error }
```

The pipeline must start with a spanset expression, so `(count() > 2) && {B}`
is a parse error.

### Narrowing

A spanset expression after an earlier stage draws its operands only from that
stage's result spans:

- `{A} | {B}` returns the spans of A's result that also match B: a
  span-level intersection, unlike the trace-level `{A} && {B}`.
- `{A} | {B} > {C}` draws both operands of `>` from A's result spans.

Ancestry and root reachability still use the whole trace: an ancestor that
failed an earlier stage still links its descendants to the root.

A narrowed stage keeps the spanset attributes (aggregate values, group keys)
of the stage before it.

### Scalar filters

```
{ .service = "api" } >> { status = error } | count() > 2
```

A scalar filter compares an aggregate over each spanset with a number or
duration literal. The aggregate is one of `count()`, `avg(x)`, `min(x)`,
`max(x)` and `sum(x)`, where `x` is a numeric [field
expression](#field-expressions), for example `avg(.a + .b)` or
`max(duration)`.

- The aggregate reads every result span of the expression before it, not
  only the last selector, and before the `spss` cap.
- A filter with no stage before it reads every span in range:
  `count() > 100` keeps traces with more than 100 spans in range.
- `| f1 | f2` keeps spansets that pass both filters.
- Spans whose argument is nil are skipped; a spanset whose argument is nil on
  every span never passes, whatever the comparison. `count() = 0` never
  matches.
- A plain number compared with a duration aggregate is in nanoseconds. Ints
  and floats compare as numbers. `avg(duration)` is truncated to whole
  nanoseconds.
- Duration literals take the units `ns`, `us`, `ms`, `s`, `m` and `h`.
  Numbers can't use exponent notation.
- `&&`, `||` and parentheses between scalar filters are parse errors. The
  aggregate must be on the left, the literal on the right, and neither side
  may contain arithmetic (`count() > 1 + 2` is rejected). A string literal on
  the right is a parse error.

Each search spanset carries the value of every aggregate it passed as a
spanset attribute named by the aggregate's canonical form (`count()`,
`avg(duration)`). Attributes accumulate across chained filters. Ints are
returned as `intValue`, floats as `doubleValue`, durations as a
`stringValue` such as `500.225µs`.

### `by()` and `coalesce()`

`by(<field expression>)` splits each trace's result spans into one spanset per
value. Spans where the expression is nil form their own group.

```
{ status = error } | by(resource.service.name) | count() > 5
```

- Consecutive `by()` stages nest: groups are keyed by the tuple of values.
- Filters after `by()` apply per group.
- Each group is returned as a separate spanset in `spanSets`, with its own
  `matched` and the key as a spanset attribute named `by(<expr>)`.
  `spanSet` is the first of them.
- Groups are ordered by key, ascending: numbers first (attribute values that
  read as a number order numerically), then booleans (`false` before `true`),
  then strings, then nil.
- `coalesce()` merges a trace's spansets back into one, without attributes.
  It drops the group keys and aggregate values of earlier stages.
- The expression must read the span: `by(1)` fails with
  `grouping field expressions must reference the span: by(1)`.

A structural operator, `&&` or `||` over grouped spansets fails with:

```
spanset operators are not supported for multiple spansets per trace. consider using coalesce()
```

A pipeline is grouped from `by()` until `coalesce()`. The check is made
before the query runs, so it rejects `({A} | by(.x)) > {B}` and
`{A} | by(.x) | {B} > {C}` even when every trace has a single group.

Group-key values: attributes are returned as `stringValue` with their stored
text, nested-set intrinsics and other number expressions as
`intValue`/`doubleValue` (or a duration `stringValue`), comparisons as
`boolValue`, and the nil group as `stringValue` `"nil"`.

### `select()`

`select(.a, resource.b, name)` adds the listed fields to every returned span
as span attributes, without filtering. It can appear at any pipeline position.

- Only attributes and intrinsics are accepted; a field expression such as
  `select(.a + .b)` is a parse error.
- Values are sorted by key. The nested-set intrinsics are returned as
  `intValue` and every other field as `stringValue`. A span without the
  field omits it; the nested-set intrinsics are never omitted. `name` sets
  the span's `name` field.
- An attribute named like a nested-set intrinsic selects the intrinsic:
  `select(.nestedSetLeft)` and `select(span.nestedSetLeft)` return the
  `nestedSetLeft` int.
- `duration`, `rootName`, `rootServiceName`, `traceDuration` and the span
  and trace ids are already part of every search result and add nothing.
- `select()` before a metrics function is accepted and has no effect;
  autocomplete ignores it too.

## Field expressions

Spanset filters `{...}` and aggregate and `by()` arguments use the field
expression language:

```
{ .a = .b }
{ .a + .b > 3 }
{ !(.a = 1) }
{ span.http.url = nil }
```

Fields:

- attributes: `span.x`, `resource.x`, `.x`
- intrinsics: `duration` (`span:duration`), `name` (`span:name`), `status`,
  `kind`, `statusMessage`, `rootName`, `rootServiceName`, and the ints
  `nestedSetLeft`, `nestedSetRight`, `nestedSetParent` (see
  [Nested-set intrinsics](#nested-set-intrinsics))

Operators, from loosest to tightest:

1. `&&`, `||` (one level, left-associative)
2. `=`, `!=`, `<`, `<=`, `>`, `>=`, `=~`, `!~`
3. `+`, `-` and unary `-`
4. unary `!`
5. `*`, `/`, `%`
6. `^` (right-associative)

So `{ .a = 1 && .b = 2 || .c = 3 }` is `{ (.a = 1 && .b = 2) || .c = 3 }`,
and `!.a = 1` is `(!.a) = 1`; write `!(.a = 1)` to negate a comparison.

Binary `-` needs whitespace: `.a-.b` is one attribute named `a-.b`, while
`.a - .b` is a subtraction.

### Types

- Attribute values are stored as text and cast at query time to the type of
  the literal or field they meet: `.a > 3` is numeric, `.a = "x"` a string,
  `.a = true` a boolean.
- Ints, floats and durations are one number type, so `.a = 200` and
  `.a = "200"` both match a stored `200`.
- Comparing two attributes compares numbers when both values are numbers,
  strings when neither is, and is false when one is a number and the other
  isn't.
- `status` and `kind` compare only with `=` and `!=`, against their own
  keywords (`status = error`, `kind = server`). `status = "error"` is a type
  error.
- An unquoted word that names no field is a string when compared with a
  field (`span.foo = bar`), and an error elsewhere.
- A filter must be boolean and an aggregate argument must be numeric. A
  filter or argument whose type can be decided before reading data fails
  early: `{ .a * .b }` (`must resolve to a boolean`), `{ !.a = 1 }` (`must
  operate on the same type`), `avg(name)` (`must resolve to a number type`).

### The nil rule

- A missing attribute, a failed cast, or integer division or modulo by zero
  yields nil, never an error. Nil propagates through arithmetic.
- Any comparison with nil is false, `!=` and `!~` included.
- `x = nil` matches spans without `x`, and `x != nil` spans with it.
  `span.a = nil`, `resource.a = nil` and `.a = nil` all test the attribute
  `a`, whatever its scope. Intrinsics are never nil: `duration = nil` is an
  error, `duration != nil` matches every span.
- `!x` and `x` in boolean context are false unless `x` is a boolean, so
  `!(.a = 1)` matches spans without `.a`.
- `&&` and `||` treat a nil operand as false.

## Nested-set intrinsics

`nestedSetLeft`, `nestedSetRight` and `nestedSetParent` describe a span's
place in its trace's tree. They are ints, accepted wherever Tempo accepts
them: spanset filters with any comparison, field expressions, `select()`,
`by()`, aggregates, and metrics `by (...)` and `*_over_time()` arguments.
They are returned as `intValue` in search spans, group keys and metrics
labels. Autocomplete doesn't offer them.

```
{ nestedSetParent < 0 } | select(nestedSetLeft, nestedSetRight)
{ nestedSetRight - nestedSetLeft > 1 }
```

The values follow Tempo's relationships, but the absolute numbers can
differ:

- A depth-first walk numbers each trace from 1, with one counter across all
  roots. A span's descendants have their `nestedSetLeft` inside the span's
  `[nestedSetLeft, nestedSetRight]`, and a leaf has
  `nestedSetRight = nestedSetLeft + 1`.
- `nestedSetParent` is the parent's `nestedSetLeft`. Roots hold `-1`.
- Spans that can't reach a root (a missing parent, a cycle, a trace without
  a root) read `0`/`0`/`0`.
- Siblings, and the roots of a trace, are numbered by start time, then span
  id.
- Spans sharing a span id form one node, placed by the earliest-starting
  copy. Every copy reports that node's values.

In filters, an attribute named like a nested-set intrinsic
(`.nestedSetLeft`, `span.nestedSetRight`) is an attribute. In `select()`
and in the filter echo it selects the intrinsic.

### The numberer

The values are computed after ingestion by a background loop, the
numberer, and stored in the `tempo_traces_nested_set` table. There is
nothing to enable; its settings are under
[Configuration](configuration.md#traceql).

- Every gigapipe instance in `all` or `writer` mode takes part, and
  ClickHouse elects one of them to run the passes through a lease in the
  `tempo_traces_nested_set_lease` table. If the leader stops, or its passes
  keep failing, another instance takes over within 4 ×
  `GIGAPIPE_NESTED_SET_NUMBERER_INTERVAL` (20s by default), so values can
  lag that much longer. `reader` instances never take part.
- On a cluster, each instance judges the lease by the clock of the
  ClickHouse node it connects to, so node clocks must agree to well within
  3 × `GIGAPIPE_NESTED_SET_NUMBERER_INTERVAL`. NTP is enough. On a
  replicated cluster, all nodes must also agree within 2s and well within
  the idle period.
- It needs **ClickHouse 24.9 or newer**. On an older server it logs an error
  and doesn't start, and every span reads `0`/`0`/`0`.
- A trace is numbered once it has received no spans for the idle period.
  With the defaults, values appear about 15s after a trace's last span. A
  late span renumbers its whole trace.
- Each pass numbers at most `GIGAPIPE_NESTED_SET_NUMBERER_MAX_SPANS` spans,
  oldest traces first; the rest wait for the next pass. On a cluster, each
  shard numbers its own traces and the cap applies per shard, so a trace's
  spans must live on one shard (see [Requirements](#requirements)).
- Each pass reads only the traces touched since the previous one. The
  leader keeps a cursor, the newest touch the last pass covered (on a
  cluster, the least across shards), in the lease table, so a new leader
  continues from it. Without one, a pass reads the last 5 minutes.
- A pass waits for synchronous inserts into its database's `tempo_traces`
  (directly or through `tempo_traces_dist`) that are still running on any
  reachable node of the cluster, and for spans a lagging replica has yet to
  fetch. While a replica has replication log entries left to pull, its shard
  numbers only traces idle for 2s longer than the idle period; while it has
  entries left to apply, only traces touched more than its delay (plus 2s
  once it is a second or more) ago. The cursor also stays at or before the
  oldest running insert's start as one of the leader instance's kept reads
  saw it, a read taken at least 2s before those entries were created.
  An instance keeps, across its terms, its newest read and, for each k
  from 0 to 11, the oldest kept read at most 2^k reads older. So a lag of
  up to about 1,000 reads (passes that read the running inserts; at least
  85 minutes at the default interval) holds that shard's numbering at most
  about three times the lag plus a pass behind. When no kept read
  qualifies (a longer lag, or an instance that has just started), that
  shard numbers nothing and the cursor holds, pass after pass. A replica that
  can't catch up (a broken part, fetches stopped), or whose tables have yet
  to receive their first insert, holds its shard and the cursor until it
  does. So the cursor doesn't pass the start of an insert whose spans a
  replica has yet to apply. A pass doesn't see an insert on a node
  that doesn't connect within 1s or complete its handshake within 2s. A node
  that then takes over 3s to answer fails the pass, and numbering resumes
  once it answers. A long insert, such as a backfill, holds back every
  shard's traces touched after it started until it ends. Async inserts are
  assumed to commit within the idle period. A trace whose spans commit later
  than that may not be numbered until it receives another span.
- While the lease can't be read or written, numbering pauses; the instances
  log the start and end of the outage. Traces whose only spans arrived more
  than about a day before numbering resumes stay `0`/`0`/`0`, or more than
  5 minutes before if no pass has succeeded yet.
- There is no backfill. A trace that receives no spans once the
  `tempo_traces_touched` table exists is never numbered, and one whose last
  span arrived more than 5 minutes before the numberer's first successful
  pass may not be.

Instances log only leadership changes: winning a term, stepping down to a
newer one, and lease outages. To see who leads and since when (on a
cluster, query `tempo_traces_nested_set_lease_dist`):

```sql
SELECT term,
       if(countIf(NOT claim) > 0, argMaxIf(holder, (expires_at, holder), NOT claim),
          argMinIf(holder, (expires_at, holder), claim)) AS leader,
       min(expires_at) - INTERVAL 15 SECOND AS since,
       max(expires_at) AS until,
       until > now64(3) AS live
FROM tempo_traces_nested_set_lease
GROUP BY term
ORDER BY term DESC
LIMIT 1
```

`leader` is the hostname, PID and a random suffix of the leading process.
`since` subtracts the lease length, 3 × the interval: `15 SECOND` holds
only at the default, so change it to match your interval. Rows are kept
for a day after they expire, so for a term led longer than that, `since`
shows about a day ago.

### Unnumbered spans

A span not numbered yet, or never, reads `0`/`0`/`0` in `select()`, the
filter echo, filters, `by()`, aggregates and metrics. Unnumbered and
unreachable spans look the same.

The root filter is the exception: `{ nestedSetParent < 0 }` and
`{ nestedSetParent = -1 }` find roots through the span's parent id, so they
match the roots of every trace, numbered or not. An unnumbered root still
selects and echoes `nestedSetParent = 0`. Only these forms, with the number
on the right, read the parent id; other spellings such as
`-1 = nestedSetParent` or `nestedSetParent <= -1` read the stored value and
miss unnumbered roots.

Structural operators never read the stored values; they resolve the tree at
query time. A span that matches `>>` can therefore carry `0`/`0`/`0`, or
stale numbers when its trace received a late span after it was numbered.

## Search results

- `spss` (spans per spanset) is the number of spans listed per spanset. It
  defaults to 3. A value that isn't a positive integer is rejected with
  `invalid spss: must be a positive number`.
- Each spanset lists its `spss` earliest-starting spans, earliest first,
  ties by span id.
- `matched` is the number of spans the spanset matched, even when `spss`
  lists fewer.
- `limit` counts traces that satisfy the whole query.
- No cap applies to the spans a query evaluates; ClickHouse memory limits
  apply.
- `durationNanos` is always the span duration in nanoseconds.

### Filter echo

Listed spans carry the attributes and intrinsics the query's spanset filters
`{...}` read, as if they were added to a `select()`.
`{ .a = 1 } >> { .b = 2 }` returns `.a` and `.b` on each listed span that has
them. The keys are the union over the whole query, and values follow the
`select()` rules above. Fields read only by a scalar filter
(`| avg(.x) > 1`) or by `by()` are not echoed. Metrics and autocomplete are
unaffected.

## Metrics

A metrics query counts exactly the spans a search for the same expression
returns. `({A} >> {B}) | rate()` counts the B spans a search for
`{A} >> {B}` returns, and `{A} || {B} | count_over_time()` counts the spans
of either side. Spans are bucketed by start time within `(from, to]`.

Scalar filters and `by()` before the metrics function decide which traces and
groups contribute; the spans of every passing group feed the metric.
`by()` groups never become series labels; use the metrics function's
`by (...)` for that. `by (...)` labels are strings, except the nested-set
intrinsics, which are `intValue`.

### Hints

`with(...)` follows the metrics function:

```
{ .service = "api" } >> { status = error } | rate() with(sample=0.1, exemplars=5)
```

- **`sample=f`** (0 < f < 1) evaluates the query over a deterministic, hashed
  sample of the data. Queries that relate spans across a trace (spanset
  operators, scalar filters, `by()`) sample whole traces by trace id, and
  every sampled trace keeps its full tree for ancestry. Other queries sample
  spans by span id. Returned values are **not** rescaled.
- **`exemplars=N`** returns at most N exemplars per series, spread evenly
  over the buckets that have one. `exemplars=false` (or N below 1) returns
  none. Without the hint, one exemplar per bucket and series is returned.
  Exemplars are always spans of the query's result, after sampling.
- Unknown hints, `sample=true`, a `sample` outside (0, 1), `most_recent`, and
  hints that don't apply to the query kind are accepted and ignored. Search
  and autocomplete ignore `sample` and `exemplars`.

## Autocomplete

Tag and value autocomplete (`/api/v2/search/tags`,
`/api/v2/search/tag/{tag}/values`) suggest only from the spans a search for
`q` returns, so suggestions narrow as a structural expression is typed.
Queries with `&&`, `||` and structural operators are supported. Without a
query, or with `{}`, the tags and values are listed unfiltered. The `{tag}`
of value autocomplete may be scoped (`span.x`, `resource.x`, `.x`); it is
looked up by its attribute name.

## Differences from Tempo

gigapipe returns the spans Tempo returns, except where listed here.

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
