# Configuration

Gigapipe is configured through environment variables. This document describes all available configuration options organized by category.

## ClickHouse Connection

Configure the connection to your ClickHouse database.

- **`CLICKHOUSE_SERVER`** - ClickHouse server hostname or IP address (default: `localhost`)
- **`CLICKHOUSE_PORT`** - ClickHouse TCP port (default: `9000`)
- **`CLICKHOUSE_DB`** - Database name (default: `cloki`)
- **`CLICKHOUSE_AUTH`** - Authentication credentials in format `username:password`
- **`CLICKHOUSE_PROTO`** - Protocol to use: `http`, `https`, or `tls` (default: `http`)
- **`SELF_SIGNED_CERT`** - Skip TLS certificate verification for self-signed certificates (`true`, `false`)

## Cluster Configuration

Enable and configure ClickHouse clustering for high-availability deployments.

- **`CLUSTER_NAME`** - Enables clustered mode and sets the cluster name. When set, gigapipe operates in distributed mode.
- **`CLICKHOUSE_READ_DIST_SUFFIX`** - Suffix for read-path distributed tables (default: `_dist`). Used for cross-cluster reads in multi-cluster deployments. See [Cross-Cluster Deployment](#cross-cluster-deployment) below.

## Authentication

Configure HTTP basic authentication for API access.

- **`QRYN_LOGIN`** - Username for HTTP basic authentication
- **`QRYN_PASSWORD`** - Password for HTTP basic authentication
- **`CLOKI_LOGIN`** - Legacy username (alias for `QRYN_LOGIN`)
- **`CLOKI_PASSWORD`** - Legacy password (alias for `QRYN_PASSWORD`)

## HTTP Settings

Configure the HTTP server behavior.

- **`PORT`** - HTTP server port (default: `3100`)
- **`HOST`** - HTTP server bind address (default: `0.0.0.0`)
- **`CORS_ALLOW_ORIGIN`** - Enable CORS and set allowed origin (e.g., `https://example.com`)

## Write Settings

Control bulk insert behavior and write performance.

- **`BULK_MAX_SIZE_BYTES`** - Maximum batch size in bytes before flushing to ClickHouse
- **`BULK_MAX_AGE_MS`** - Maximum age in milliseconds before flushing batch (default: `100`)

## Advanced Settings

Fine-tune gigapipe behavior for specific use cases.

- **`ADVANCED_SAMPLES_ORDERING`** - Custom ordering for samples table (ClickHouse ORDER BY clause)
- **`ADVANCED_PROMETHEUS_MAX_SAMPLES`** - Maximum number of samples returned in Prometheus queries
- **`ADVANCED_OMIT_EMPTY_VALUES`** - Omit empty values in query results (`true`, `false`)
- **`OMIT_CREATE_TABLES`** - Skip table creation on startup (`true`, `false`)
- **`COMPAT_4_0_19`** - Enable compatibility mode for v4.0.19 behavior (`true`, `false`)

## Storage and Retention

Configure data retention and storage policies.

- **`SAMPLES_DAYS`** - TTL in days for stored samples (default: `7`)
- **`STORAGE_POLICY`** - ClickHouse storage policy name for data placement

## Mode

Control which components run in this instance.

- **`MODE`** - Operating mode:
  - `all` - Run both reader and writer (default)
  - `reader` - Run query/read endpoints only
  - `writer` - Run ingestion/write endpoints only
  - `init_only` - Initialize database and exit
- **`READONLY`** - Set to `true` to force reader mode (equivalent to `MODE=reader`)

## Logging

Configure logging output.

- **`LOG_LEVEL`** - Log level: `debug`, `info`, `warn`, `error`

## Log Drilldown

Enable pattern detection and log analysis features.

- **`LOG_DRILLDOWN`** - Enable log pattern detection and drilldown features (`true`, `false`)
- **`LOG_PATTERN_SIMILARITY`** - Similarity threshold for pattern grouping, range 0-1 (default: `0.7`). Higher values require more similarity.
- **`LOG_PATTERN_READ_LIMIT`** - Maximum number of log patterns to read per request (default: `300`)

## Self-Profiling

Enable continuous profiling of gigapipe itself using Pyroscope.

- **`PYROSCOPE_SERVER_ADDRESS`** - Pyroscope server URL (e.g., `http://pyroscope:4040`)
- **`PYROSCOPE_APPLICATION_NAME`** - Application name in Pyroscope (default: `gigapipe`)

## Cross-Cluster Deployment

For multi-region or multi-cluster deployments, you can separate write and read paths using different distributed table configurations.

### Use Case

Use cross-cluster reads when you need to:
- Query data across multiple ClickHouse clusters
- Implement multi-region deployments with centralized querying
- Separate read and write workloads

### Architecture

Writes use local distributed tables (default `_dist` suffix) that target the local cluster. Reads can use tables with a custom suffix (configured via `CLICKHOUSE_READ_DIST_SUFFIX`) that target multiple clusters.

This allows queries to fan out across clusters while keeping writes local to each cluster.

### Configuration Example

```bash
# Writer instances (local cluster writes)
MODE=writer
CLUSTER_NAME=local_cluster
CLICKHOUSE_SERVER=clickhouse-local.example.com

# Reader instances (cross-cluster reads)
MODE=reader
CLUSTER_NAME=local_cluster
CLICKHOUSE_SERVER=clickhouse-local.example.com
CLICKHOUSE_READ_DIST_SUFFIX=_dist_cross_cluster
```

In this setup:
- Write operations use `table_name_dist` (local cluster only)
- Read operations use `table_name_dist_cross_cluster` (can span multiple clusters)

### ClickHouse Setup

On the ClickHouse side, create distributed tables with the custom suffix:

```sql
CREATE TABLE IF NOT EXISTS samples_v3_dist_cross_cluster ON CLUSTER '{cluster}'
AS samples_v3
ENGINE = Distributed(
  'cross_cluster_name',
  currentDatabase(),
  'samples_v3',
  rand()
)
SETTINGS skip_unavailable_shards = 1;
```

The `skip_unavailable_shards=1` setting ensures queries continue even if some shards are temporarily unavailable.

### Backward Compatibility

Without setting `CLICKHOUSE_READ_DIST_SUFFIX`, gigapipe uses the default `_dist` suffix for both reads and writes, maintaining backward compatibility with existing deployments.

## Example Configuration

Minimal single-node setup:

```bash
CLICKHOUSE_SERVER=clickhouse.example.com
CLICKHOUSE_PORT=9000
CLICKHOUSE_DB=gigapipe
CLICKHOUSE_AUTH=admin:password
PORT=3100
```

Production clustered setup:

```bash
CLICKHOUSE_SERVER=clickhouse.example.com
CLICKHOUSE_PORT=9000
CLICKHOUSE_DB=gigapipe
CLICKHOUSE_AUTH=admin:password
CLUSTER_NAME=production_cluster
STORAGE_POLICY=tiered_storage
SAMPLES_DAYS=30
BULK_MAX_AGE_MS=100
PORT=3100
QRYN_LOGIN=admin
QRYN_PASSWORD=secure_password
LOG_LEVEL=info
```
