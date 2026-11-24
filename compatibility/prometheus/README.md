# PromQL parity with Prometheus

Runs the [PromQL compliance suite](https://github.com/prometheus/compliance/tree/main/promql) with a real Prometheus as the reference and gigapipe as the target. Every compliance query is sent to both, and a query whose answers differ shows up as a failing subtest.

```
make parity-prom
```

This needs Docker and Go. `run.sh`:

1. downloads Prometheus 3.15.1 (the version gigapipe's `go.mod` uses) into `bin/` and checks its sha256
2. starts ClickHouse and gigapipe built from this tree (the `test/integration` stack)
3. runs `go test -json` here and prints how many queries agree and differ; per-query results are in `out/parity.json`

`make parity-prom` is report-only: differences don't fail it.

## Baseline

`baseline.json` records every query's state (`agree` or `differ`) and, for each query that differs, a `reason`.

```
make parity-prom-check      # fails if any query changed state since baseline.json
make parity-prom-baseline   # rewrites baseline.json from a fresh run
```

`parity-prom-check` fails when a query that agreed now differs, a query that differed now agrees, a baselined query didn't run, or a new query isn't in the baseline yet. It lists each one.

`parity-prom-baseline` keeps the reasons already in the file and gives every new difference the reason `unclassified`. Replace it with the real reason (an issue link, or why the difference is expected) before committing.

The comparison itself is in `compatibility/ratchet/`, shared with the other parity harnesses. It also runs on a saved result, from the repository root: `go run ./compatibility/ratchet check -results compatibility/prometheus/out/parity.json -baseline compatibility/prometheus/baseline.json`.

## How the data gets in

The suite replays a recorded 70-minute dataset (`demo_*` metrics) through a scrape endpoint, and Prometheus scrapes it. Gigapipe doesn't scrape, so the test's gigapipe target scrapes the replayer itself and sends the samples to `/api/v1/prom/remote/write`, then forwards the suite's queries to gigapipe.

Native histograms aren't sent (remote write here carries classic float samples), so the native-histogram queries differ.

## Settings

- `GIGAPIPE_URL`: where the test sends data and queries. `run.sh` always starts its own gigapipe on `http://localhost:3100`, so leave `GIGAPIPE_URL` unset when using it. To test a gigapipe that is already running elsewhere, skip `run.sh` and run `go test -v ./...` in this folder with `GIGAPIPE_URL` and `PROMETHEUS_BINARY` set.
- `PARITY_KEEP_SERVICE_NAME=1`: also compare the `service_name` label, which gigapipe adds to every series and Prometheus doesn't. Left out by default.

## Why a separate module

The suite and its dependencies are only needed here, so this folder has its own `go.mod`. Gigapipe's `go.mod` and binary don't change, and `go test ./...` from the repository root doesn't include this folder.
