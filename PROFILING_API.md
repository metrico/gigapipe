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

Query available label names for profiles.

```
POST /querier.v1.QuerierService/LabelNames
```

**Parameters**:
- `matchers` - Array of label selectors
- `start` - Start time in milliseconds
- `end` - End time in milliseconds

### Label Values

Query values for a specific label.

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

**Response**: Time series with timestamps and values.

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

**Response**: Flamegraph data combining samples by function name.

## Render Endpoints

### Render Flamegraph

Render profiles as flamegraph visualization data.

```
GET /pyroscope/render?query={selector}&from={timestamp}&until={timestamp}
```

**Query parameters**:
- `query` - Profile query in format `profile_type{label_selector}`
- `from` - Start timestamp in milliseconds
- `until` - End timestamp in milliseconds
- `format` - Output format: `dot` for Graphviz DOT format, omit for JSON flamegraph (default)
- `maxNodes` - Limit nodes in output (0 = unlimited, only for DOT format)

**Example**:
```bash
curl "http://localhost:3100/pyroscope/render?query=process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name=\"my-app\"}&from=1704067200000&until=1704153600000"
```

**Response**: Flamegraph JSON suitable for visualization.

### Render Diff

Compare two profiles side-by-side.

```
GET /pyroscope/render-diff?leftQuery={query}&leftFrom={ts}&leftUntil={ts}&rightQuery={query}&rightFrom={ts}&rightUntil={ts}
```

**Query parameters**:
- `leftQuery` - First profile query
- `leftFrom` - First profile start timestamp in milliseconds
- `leftUntil` - First profile end timestamp in milliseconds
- `rightQuery` - Second profile query
- `rightFrom` - Second profile start timestamp in milliseconds
- `rightUntil` - Second profile end timestamp in milliseconds

**Response**: Diff flamegraph showing differences between the two profiles.

## DOT Format Export

Export profile data as Graphviz DOT format for external visualization or AI analysis.

### Overview

The DOT format rendering converts flamegraph data into a directed graph suitable for:
- Exporting to external visualization tools
- Generating profile diagrams with Graphviz
- AI-assisted profile analysis
- Documentation and reporting

### Using DOT Format

Add `format=dot` to the render endpoint:

```bash
GET /pyroscope/render?query={selector}&from={timestamp}&until={timestamp}&format=dot
```

**Response**: `text/vnd.graphviz; charset=utf-8` content type with DOT graph data.

### DOT Output Features

The generated DOT format includes:

- **Node labels**: Function names with total and self sample counts and percentages
- **Heat colors**: Visual intensity from light gray (0% self samples) to red (100% self samples)
- **Edge weights**: Proportional to sample share (1-100 scale)
- **Font scaling**: Node font size scales 8-24pt based on self-sample percentage
- **Human-readable values**: Automatic formatting based on unit type (e.g., "1.23s" for nanoseconds, "1.23 MB" for bytes)

### Limiting Graph Size

Use `maxNodes` to control output complexity:

```bash
curl "http://localhost:3100/pyroscope/render?query=process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name=\"app\"}&from=1704067200000&until=1704153600000&format=dot&maxNodes=50"
```

This limits the output to the top 50 nodes by total sample count, making large profiles more manageable.

### Visualization Workflow

Generate a PNG diagram using Graphviz:

```bash
curl "http://localhost:3100/pyroscope/render?query=process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name=\"my-app\"}&from=1704067200000&until=1704153600000&format=dot" | dot -Tpng > profile.png
```

Generate an SVG for interactive viewing:

```bash
curl "http://localhost:3100/pyroscope/render?query=process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name=\"my-app\"}&from=1704067200000&until=1704153600000&format=dot" | dot -Tsvg > profile.svg
```

## Query Syntax

### Profile Type ID Format

Profile type IDs follow the format:

```
name:sample_type:sample_unit:period_type:period_unit
```

**Examples**:
- `process_cpu:cpu:nanoseconds:cpu:nanoseconds` - CPU profiling
- `memory:alloc_space:bytes:space:bytes` - Memory allocation
- `goroutine:goroutine:count:goroutine:count` - Goroutine counts

### Label Selector Syntax

Use Pyroscope-style label selectors to filter profiles:

```
{label1="value1", label2="value2"}
```

**Examples**:
- `{service_name="api-server"}` - Single label match
- `{service_name="api-server", env="production"}` - Multiple labels
- `{service_name=~"api.*"}` - Regex matching (if supported)

### Complete Query Format

Combine profile type and label selector:

```
process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name="my-app", env="prod"}
```

## Response Formats

### Flamegraph JSON

Default response format for `/pyroscope/render`:

```json
{
  "version": 1,
  "flamebearerProfileV1": {
    "flamebearer": {
      "names": ["total", "main", "handler", ...],
      "levels": [[0, 1000, 0, 0], ...],
      "numTicks": 1000,
      "maxSelf": 500
    },
    "metadata": {
      "format": "single",
      "sampleRate": 100,
      "units": "samples",
      "name": "cpu"
    }
  }
}
```

### DOT Format

Response format for `format=dot`:

```dot
digraph "process_cpu (cpu, nanoseconds)" {
  // Profile: process_cpu:cpu:nanoseconds:cpu:nanoseconds  Total: 1.23s
  node [shape=box];
  edge [fontsize=10];
  N0 [label="total\n1.23s (100%)" fontsize=12 style=filled fillcolor="#eeeeee"];
  N1 [label="main\ntotal: 1.20s (97.6%) self: 0.10s (8.1%)" fontsize=10 style=filled fillcolor="#f0f0f0"];
  N0 -> N1 [label="97.6%" weight=98];
  ...
}
```

## Use Cases

### Performance Analysis

Query CPU profiles to identify hotspots:

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

### Export for Documentation

Generate profile diagrams for documentation:

```bash
curl "http://localhost:3100/pyroscope/render?query=process_cpu:cpu:nanoseconds:cpu:nanoseconds{service=\"api\"}&from=1704067200000&until=1704153600000&format=dot&maxNodes=30" | \
  dot -Tpng -Gdpi=150 > cpu-profile.png
```

### AI-Assisted Analysis

Export DOT format for analysis by AI tools:

```bash
curl "http://localhost:3100/pyroscope/render?query=process_cpu:cpu:nanoseconds:cpu:nanoseconds{service=\"app\"}&from=1704067200000&until=1704153600000&format=dot" > profile.dot
# Feed profile.dot to AI for analysis
```
