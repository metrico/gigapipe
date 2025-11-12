# Proposal: Implement Prometheus Metadata API Support

## Problem Statement

Currently, the `/api/v1/metadata` endpoint in gigapipe returns an empty response, which doesn't fully implement the Prometheus Metadata API specification. This endpoint should return metadata about metrics including:
- `type` (counter, gauge, histogram, summary, untyped)
- `help` (description of the metric)
- `unit` (unit of measurement)

### Current Issues

1. **Endpoint Implementation**: The `/api/v1/metadata` endpoint exists but returns only `{"status": "success", "data": {}}` without any actual metadata.

2. **Data Loss During Write**: 
   - Prometheus Remote Write protocol (current version) doesn't include metadata in the `WriteRequest` protobuf
   - Even when metadata is provided via HTTP header `X-Scope-Meta`, it's extracted but not persisted to the database
   - The `Meta` field exists in `TimeSeriesRequest` struct but is not used in INSERT queries

3. **Database Schema**: 
   - The `time_series` table doesn't have columns for storing metric metadata
   - Metadata is lost during the write process

4. **No Documentation**: There's no documentation about metadata support in the codebase

### Impact

- Incompatibility with Prometheus API specification
- Missing functionality for tools that rely on metric metadata
- Loss of valuable metadata information during ingestion

## Proposed Solution

### Overview

Implement a **dedicated table approach** using a new `metrics_meta` table to store metric metadata. This solution:
- Clean separation of concerns (dedicated table for metadata)
- Simple schema optimized for metadata queries
- Direct access by metric name (no JOINs required)
- Maintains backward compatibility

### Architecture

#### Data Storage

Use a new `metrics_meta` table with the following structure:
```sql
CREATE TABLE metrics_meta (
    metric_name String,    -- Name of the metric (e.g., "http_requests_total")
    value String,          -- JSON: {"type": "counter", "help": "...", "unit": "seconds"}
    timestamp_ms Int64     -- Timestamp in milliseconds for ReplacingMergeTree deduplication
) ENGINE = ReplacingMergeTree(timestamp_ms) 
ORDER BY metric_name;
```

**Storage Format**:
- `metric_name`: Direct metric name (no fingerprint lookup needed)
- `value`: JSON string containing `{"type": "...", "help": "...", "unit": "..."}`
- `timestamp_ms`: Timestamp in milliseconds for ReplacingMergeTree deduplication

#### Data Flow

1. **Write Path** (`/api/v1/prom/remote/write`):
   ```
   HTTP Request → Parser → Extract Metadata from Labels → Store in metrics_meta table
   ```

2. **Read Path** (`/api/v1/metadata`):
   ```
   API Request → Query metrics_meta table → Format Response
   ```

### Implementation Details

#### Phase 1: Metadata Ingestion

1. **Update Prometheus Remote Write Parser** (`writer/utils/unmarshal/builder.go`):
   - Extract metadata from special labels: `__metric_type__`, `__metric_help__`, `__metric_unit__`
   - Remove these labels before storing in `time_series`
   - Store metadata in `MetadataData` structure

2. **Create Metadata Service** (`writer/service/insert/metadata.go`):
   - New service to insert metadata into `metrics_meta` table
   - Handle JSON serialization of metadata
   - Batch inserts for performance
   - Use `ch-go` native protocol with `Int64` for timestamps

3. **Update Write Handler** (`writer/controller/builder.go`):
   - Integrate metadata service into write pipeline
   - Extract metadata from labels and persist to `metrics_meta` table

#### Phase 2: Metadata Retrieval

1. **Implement Metadata Query Service** (`reader/service/metadata.go`):
   - Query `metrics_meta` table for metadata
   - Direct filtering by metric name (no fingerprint lookup needed)
   - Support `limit` parameter
   - Use `argMax(value, timestamp_ms)` for deduplication

2. **Update Metadata Endpoint** (`reader/controller/prom_query_labels.go`):
   - Parse query parameters: `metric`, `limit`
   - Call metadata service
   - Format response according to Prometheus API spec

#### Phase 3: API Specification Compliance

**Prometheus API Format**:
```json
{
  "status": "success",
  "data": {
    "metric_name_1": [
      {
        "type": "counter",
        "help": "Description of the metric",
        "unit": ""
      }
    ],
    "metric_name_2": [
      {
        "type": "gauge",
        "help": "Another description",
        "unit": "seconds"
      }
    ]
  }
}
```

**Query Parameters**:
- `metric=<metric_name>`: Filter by metric name
- `limit=<number>`: Limit number of results

### Code Changes

#### Files to Modify

1. **Writer Side**:
   - `writer/utils/unmarshal/builder.go` - Extract metadata from labels
   - `writer/utils/unmarshal/shared.go` - Store metadata in `MetadataData`
   - `writer/controller/builder.go` - Pass metadata to service
   - `writer/service/insert/metadata.go` - **NEW** - Metadata insertion service
   - `writer/model/metadata.go` - **NEW** - `MetadataData` structure

2. **Reader Side**:
   - `reader/controller/prom_query_labels.go` - Implement Metadata endpoint
   - `reader/service/metadata.go` - **NEW** - Metadata query service
   - `reader/router/prometheus_labels.go` - Already registered

3. **Database**:
   - New table: `metrics_meta` (see schema above)
   - Distributed table: `metrics_meta_dist` for cluster setups

#### New Files

1. `writer/service/insert/metadata.go` - Metadata insertion service
2. `reader/service/metadata.go` - Metadata query service
3. `writer/utils/metadata/parser.go` - Metadata parsing utilities

### Metadata Sources

The implementation currently supports metadata from labels:

1. **Labels** (current implementation):
   - Extract from special labels: `__metric_type__`, `__metric_help__`, `__metric_unit__`
   - These labels are automatically removed before storing in `time_series`
   - Example: Labels `[["__name__", "http_requests_total"], ["__metric_type__", "counter"], ["__metric_help__", "Total HTTP requests"], ["__metric_unit__", "requests"]]`

**Future enhancements** (not yet implemented):
- HTTP Header `X-Scope-Meta` support
- Prometheus Remote Write Metadata (when protobuf supports it)

### Backward Compatibility

- Existing metrics without metadata will continue to work
- `/api/v1/metadata` will return empty data for metrics without metadata
- No breaking changes to existing APIs
- Graceful degradation when metadata is not available

### Performance Considerations

1. **Write Path**:
   - Batch metadata inserts (similar to time_series inserts)
   - Use async inserts if configured
   - Minimal overhead (one additional INSERT per unique metric)

2. **Read Path**:
   - Direct queries by `metric_name` (no JOINs needed)
   - Use ReplacingMergeTree for efficient deduplication
   - Index on `metric_name` for fast lookups
   - `argMax(value, timestamp_ms)` ensures latest metadata

3. **Storage**:
   - ReplacingMergeTree ensures only latest metadata is kept
   - JSON format allows flexible schema without migrations
   - Low storage overhead (~100-200 bytes per metric)
   - `Int64` timestamp avoids `DateTime64` conversion issues with `ch-go`

### Testing Strategy

1. **Unit Tests**:
   - Metadata parsing from different sources
   - JSON serialization/deserialization
   - Query parameter handling

2. **Integration Tests**:
   - End-to-end write and read flow
   - Multiple metadata sources
   - Concurrent writes

3. **Compatibility Tests**:
   - Verify Prometheus API format compliance
   - Test with Prometheus clients
   - Verify backward compatibility

### Migration Plan

1. **Phase 1** (Week 1):
   - Implement metadata ingestion
   - Add metadata service
   - Update write pipeline

2. **Phase 2** (Week 2):
   - Implement metadata retrieval
   - Update `/api/v1/metadata` endpoint
   - Add caching

3. **Phase 3** (Week 3):
   - Testing and bug fixes
   - Documentation
   - Performance optimization

### Future Enhancements

1. **Prometheus 2.23.0+ Support**:
   - Update protobuf definitions when available
   - Native metadata support in remote write

2. **Metadata Management API**:
   - POST/PUT endpoints to update metadata
   - Bulk metadata import/export

3. **Metadata Discovery**:
   - Automatic metadata extraction from metric names
   - Integration with Prometheus exporters

### Risks and Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| Performance degradation | Medium | Implement caching, batch inserts |
| Storage overhead | Low | Use ReplacingMergeTree, JSON compression |
| Backward compatibility | Low | Graceful degradation, optional feature |
| Metadata format changes | Low | JSON allows flexible schema |

### Success Criteria

- [x] `/api/v1/metadata` returns proper Prometheus API format
- [x] Metadata persists through write/read cycle
- [x] Supports `metric` and `limit` query parameters
- [x] Backward compatible with existing metrics
- [x] Performance impact < 5% on write path
- [x] Documentation updated

### References

- [Prometheus Metadata API Specification](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metric-metadata)
- [Prometheus Remote Write Protocol](https://prometheus.io/docs/concepts/remote_write_spec/)
- Existing `settings` table usage in codebase

---

## Implementation Checklist

### Writer Side
- [x] Create `writer/service/insert/metadata.go`
- [x] Create `writer/utils/metadata/parser.go`
- [x] Update `writer/utils/unmarshal/builder.go` (extract from labels)
- [x] Update `writer/utils/unmarshal/shared.go` (MetadataData structure)
- [x] Update `writer/controller/builder.go`
- [x] Create `writer/model/metadata.go`
- [x] Add unit tests for metadata parsing

### Reader Side
- [x] Create `reader/service/metadata.go`
- [x] Update `reader/controller/prom_query_labels.go`
- [x] Implement query parameter parsing
- [x] Add unit tests for metadata queries

### Database
- [x] Create `metrics_meta` table schema
- [x] Create `metrics_meta_dist` distributed table

### Integration
- [x] End-to-end integration tests
- [x] Performance benchmarks
- [x] Documentation updates

---

**Author**: Development Team  
**Date**: 2024  
**Status**: ✅ Implemented

