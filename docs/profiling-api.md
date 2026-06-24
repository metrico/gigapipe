# Profiling API

Gigapipe implements the Pyroscope API for continuous profiling with support for querying, rendering, and exporting profile data.

## Profile Endpoints

### Profile Types

List available profile types in your data.

```
POST /querier.v1.QuerierService/ProfileTypes
```

**Request body**:
```json
{
  "start": 1704067200000,
  "end": 1704153600000
}
```

**Response**: Returns profile types in format `name:sample_type:sample_unit:period_type:period_unit`.

### Label Names

```
POST /querier.v1.QuerierService/LabelNames
```

**Parameters**:
- `matchers` - Array of label selectors
- `start` - Start time in milliseconds
- `end` - End time in milliseconds

### Label Values

```
POST /querier.v1.QuerierService/LabelValues
```

**Parameters**:
- `matchers` - Array of label selectors
- `name` - Label name to query
- `start` - Start time in milliseconds
- `end` - End time in milliseconds

### Select Series

Query profile time series data.

```
POST /querier.v1.QuerierService/SelectSeries
```

**Parameters**:
- `profileTypeID` - Profile type ID
- `labelSelector` - Label selector query
- `start` - Start time in milliseconds
- `end` - End time in milliseconds
- `groupBy` - Array of labels to group by
- `step` - Query resolution in seconds
- `aggregation` - Aggregation type (sum, avg, etc.)

### Merge Stack Traces

Aggregate profiles into flamegraph format.

```
POST /querier.v1.QuerierService/SelectMergeStacktraces
```

**Parameters**:
- `profileTypeID` - Profile type ID
- `labelSelector` - Label selector query
- `start` - Start time in milliseconds
- `end` - End time in milliseconds
- `maxNodes` - Optional limit on number of nodes returned

## Render Endpoints

### Render Flamegraph

```
GET /pyroscope/render?query={selector}&from={timestamp}&until={timestamp}
```

**Query parameters**:
- `query` - Profile query in format `profile_type{label_selector}`
- `from` - Start timestamp in milliseconds
- `until` - End timestamp in milliseconds
- `format` - Output format: `dot` for Graphviz DOT format, omit for JSON flamegraph (default)
- `maxNodes` - Limit nodes in output (0 = unlimited, only applies to DOT format)

**Example**:
```bash
curl "http://localhost:3100/pyroscope/render?query=process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name=\"my-app\"}&from=1704067200000&until=1704153600000"
```

### Render Diff

Compare two profiles side-by-side.

```
GET /pyroscope/render-diff?leftQuery={query}&leftFrom={ts}&leftUntil={ts}&rightQuery={query}&rightFrom={ts}&rightUntil={ts}
```

## DOT Format Export

Export profile data as Graphviz DOT format for external visualization or AI analysis.

Add `format=dot` to the render endpoint:

```bash
GET /pyroscope/render?query={selector}&from={timestamp}&until={timestamp}&format=dot
```

**Response content type**: `text/vnd.graphviz; charset=utf-8`

### DOT Output Features

- **Node labels**: Function names with total and self sample counts and percentages
- **Heat colors**: Light gray (0% self samples) to red (100% self samples)
- **Edge weights**: Proportional to sample share (1-100 scale)
- **Font scaling**: Node font size 8-24pt based on self-sample percentage
- **Human-readable values**: Auto-formatted by unit type (e.g., `1.23s` for nanoseconds, `1.23 MB` for bytes)

### Limiting Graph Size

Use `maxNodes` to control output complexity:

```bash
curl "http://localhost:3100/pyroscope/render?query=process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name=\"app\"}&from=1704067200000&until=1704153600000&format=dot&maxNodes=50"
```

### Visualization Workflow

Generate a PNG with Graphviz:

```bash
curl "http://localhost:3100/pyroscope/render?query=process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name=\"my-app\"}&from=1704067200000&until=1704153600000&format=dot" | dot -Tpng > profile.png
```

Generate SVG:

```bash
curl "http://localhost:3100/pyroscope/render?query=process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name=\"my-app\"}&from=1704067200000&until=1704153600000&format=dot" | dot -Tsvg > profile.svg
```

## Query Syntax

### Profile Type ID Format

```
name:sample_type:sample_unit:period_type:period_unit
```

**Examples**:
- `process_cpu:cpu:nanoseconds:cpu:nanoseconds` - CPU profiling
- `memory:alloc_space:bytes:space:bytes` - Memory allocation
- `goroutine:goroutine:count:goroutine:count` - Goroutine counts

### Label Selector Syntax

```
{label1="value1", label2="value2"}
```

**Examples**:
- `{service_name="api-server"}` - Single label
- `{service_name="api-server", env="production"}` - Multiple labels

### Complete Query Format

```
process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name="my-app", env="prod"}
```

## Use Cases

### Performance Analysis

```bash
curl -X POST http://localhost:3100/querier.v1.QuerierService/SelectMergeStacktraces \
  -H "Content-Type: application/json" \
  -d '{
    "profileTypeID": "process_cpu:cpu:nanoseconds:cpu:nanoseconds",
    "labelSelector": "{service_name=\"api\"}",
    "start": 1704067200000,
    "end": 1704153600000
  }'
```

### Memory Leak Detection

Compare memory profiles over time:

```bash
curl "http://localhost:3100/pyroscope/render-diff?leftQuery=memory:inuse_space:bytes:space:bytes{app=\"service\"}&leftFrom=1704067200000&leftUntil=1704070800000&rightQuery=memory:inuse_space:bytes:space:bytes{app=\"service\"}&rightFrom=1704153600000&rightUntil=1704157200000"
```

### Export for AI Analysis

```bash
curl "http://localhost:3100/pyroscope/render?query=process_cpu:cpu:nanoseconds:cpu:nanoseconds{service=\"app\"}&from=1704067200000&until=1704153600000&format=dot" > profile.dot
```
