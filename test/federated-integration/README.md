# Federated integration stack

A fully self-contained Docker Compose stack for exercising gigapipe's
**federated (multi-tenant)** mode end to end: its **own ephemeral ClickHouse**,
a split writer/reader, a dummy log producer, and a Grafana Alloy collector.
Nothing external is required.

```
dummy-app ──writes──▶ /var/log/app/app.log
                              │  (shared volume)
                          Alloy tails, adds X-Scope-OrgID: platform
                              │
                              ▼
                   gigapipe-writer (MODE=writer, :3100)  ──▶ ClickHouse (ephemeral,
                              ▲                                cluster `default`,
                   gigapipe-reader (MODE=reader, :3101)  ◀──  federated schema, oid column)
                              │
                     you query with X-Scope-OrgID
```

Services:

| # | Service | Role | Host address |
|---|---------|------|--------------|
| 0 | `clickhouse` | Ephemeral single-node cluster `default` (v25.3.14.14) with embedded Keeper | `0.0.0.0:9000` (native), `0.0.0.0:8124` (HTTP) |
| 1 | `gigapipe-writer` | Ingestion layer (`MODE=writer`) — runs migrations, accepts pushes | `0.0.0.0:3100` |
| 2 | `gigapipe-reader` | Query layer (`MODE=reader`) | `0.0.0.0:3101` |
| 3 | `dummy-app` | Emits one logfmt line/second (levels rotate info/warn/error) | — (internal) |
| 4 | `alloy` | Tails the dummy app's file, pushes to the writer as tenant `platform` | `0.0.0.0:12345` (debug UI) |

Both gigapipe services bind `HOST=0.0.0.0`, so the APIs are reachable via
`localhost` and via your devvm's fixed IP (e.g. `http://<devvm-ip>:3100`).

## Prerequisites

- Docker + Docker Compose v2. That's it — the ClickHouse cluster is created and
  torn down with the stack.

## Ephemeral ClickHouse

The `clickhouse` service runs `clickhouse/clickhouse-server:25.3.14.14` with a
small override in `clickhouse/config.d/cluster.xml` that makes a single node
behave as the cluster gigapipe expects:

- a `default` cluster (one shard, one replica) so `CREATE ... ON CLUSTER default`
  and Distributed tables work;
- embedded ClickHouse **Keeper** (the distributed-DDL queue);
- macros (`shard`/`replica`/`cluster`);
- a `tiered_nvme_s3` storage policy that matches the writer's `STORAGE_POLICY`
  env — here it is just the default disk (no real S3 tiering), which keeps the
  stack self-contained.

Its data lives in the `clickhousedata` named volume, removed by `down -v`.

## Run

```bash
cd test/federated-integration
docker compose up --build
```

Boot order is enforced by health/dependency: ClickHouse becomes healthy →
the writer builds the image (repo root `../..` + top-level `Dockerfile`) and
applies the **federated** schema to the `gigapipe` database (the `oid` column,
with `oid` prepended to each table's primary key) → the reader (which reuses the
writer's image and does not migrate) and Alloy start.

## Verify tenancy end to end

Federation is on (`FEDERATED=1`), so the writer requires `X-Scope-OrgID` and the
reader filters by it (anchored regex match).

1. **Ingest is flowing** (Alloy → writer, tenant `platform`). Wait ~30s, then
   query the reader **as the same tenant**:

   ```bash
   curl -s -H 'X-Scope-OrgID: platform' \
     'http://localhost:3101/loki/api/v1/query_range' \
     --data-urlencode 'query={service="dummy-app"}' \
     --data-urlencode "start=$(date -u -d '-10 min' +%s)000000000" \
     --data-urlencode "end=$(date -u +%s)000000000" | jq '.data.result | length'
   ```

   Expect a non-zero number of streams.

2. **Wrong tenant sees nothing.** Same query with a different tenant returns an
   empty result set:

   ```bash
   curl -s -H 'X-Scope-OrgID: data' \
     'http://localhost:3101/loki/api/v1/query_range' \
     --data-urlencode 'query={service="dummy-app"}' \
     --data-urlencode "start=$(date -u -d '-10 min' +%s)000000000" \
     --data-urlencode "end=$(date -u +%s)000000000" | jq '.data.result'
   ```

3. **Regex / multi-tenant match.** `platform|data` matches either tenant:

   ```bash
   curl -s -H 'X-Scope-OrgID: platform|data' \
     'http://localhost:3101/loki/api/v1/query_range' \
     --data-urlencode 'query={service="dummy-app"}' \
     --data-urlencode "start=$(date -u -d '-10 min' +%s)000000000" \
     --data-urlencode "end=$(date -u +%s)000000000" | jq '.data.result | length'
   ```

4. **Missing tenant returns empty instantly** (no ClickHouse query on the read
   side; the writer would 400 a push):

   ```bash
   curl -s 'http://localhost:3101/loki/api/v1/query_range' \
     --data-urlencode 'query={service="dummy-app"}' \
     --data-urlencode "start=$(date -u -d '-10 min' +%s)000000000" \
     --data-urlencode "end=$(date -u +%s)000000000"
   # -> {"status":"success","data":{"resultType":"streams","result":[]}}
   ```

5. **Writer rejects an untenanted push** (what Alloy must never do under
   federation):

   ```bash
   curl -s -o /dev/null -w '%{http_code}\n' -XPOST \
     'http://localhost:3100/loki/api/v1/push' \
     -H 'Content-Type: application/json' \
     --data '{"streams":[{"stream":{"service":"x"},"values":[["'"$(date +%s)"'000000000","hi"]]}]}'
   # -> 400
   ```

Confirm at the storage layer that every row carries a tenant (query the
ephemeral ClickHouse directly over its host-exposed HTTP port):

```bash
curl -s 'http://localhost:8124/' \
  --data-binary 'SELECT DISTINCT oid FROM gigapipe.samples_v3 FORMAT TSV'
# -> platform   (never an empty string)

# Or, to see oid in the sorting key:
curl -s 'http://localhost:8124/' \
  --data-binary 'SHOW CREATE TABLE gigapipe.samples_v3 FORMAT TSVRaw'
```

## Endpoints

- Writer (ingest): `http://localhost:3100` — `/loki/api/v1/push`, `/v1/logs`
  (OTLP), etc. Health: `GET /ready`.
- Reader (query): `http://localhost:3101` — `/loki/api/v1/query_range`,
  `/loki/api/v1/labels`, `/loki/api/v1/series`. Health: `GET /ready`.
- Alloy debug UI: `http://localhost:12345`.
- ClickHouse: native `localhost:9000`, HTTP `http://localhost:8124`
  (`/ping`, or POST SQL to `/`).

Point Grafana's Loki datasource at `http://<host>:3101` and set the custom HTTP
header `X-Scope-OrgID: platform` to browse the ingested logs.

## Change the tenant

Alloy ships as tenant `platform` (`tenant_id` in `alloy/config.alloy`). Edit
that value (or add more `loki.write` blocks with different `tenant_id`s) to
simulate multiple tenants, then re-run `docker compose up -d`.

## Teardown

```bash
docker compose down -v
```
