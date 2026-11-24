# LogQL parity with Loki

Sends the LogQL queries in `queries.txt` to a real Loki and to gigapipe, both fed the same logs, and compares the answers. A query answered differently shows up as a failing subtest of `TestLogQLParity`.

```
make parity-logql            # report: how many queries agree and differ
make parity-logql-check      # fails if any query changed state since baseline.json
make parity-logql-baseline   # rewrites baseline.json from a fresh run
```

This needs Docker and Go. `run.sh`:

1. builds the checker (`compatibility/ratchet`, shared with the PromQL harness)
2. starts Loki 3.7.10 (pinned by image digest) with `loki.yaml`, on port 3101
3. starts ClickHouse and gigapipe built from this tree (the `test/integration` stack), on port 3100
4. runs `go test -tags parity` here; per-query results are in `out/parity.json`
5. hands them to the checker, which prints the counts and, for `check`, compares them with `baseline.json`

## The data

The M2 seed fixture (`test/integration/seed`): 4 log streams, checkout pods p1, p2 and p3 with 4, 7 and 1 lines and payments p1 with 5, 17 lines in all, 10 seconds apart. Each line is a JSON object with `level`, `msg`, `latency_ms` and `seq`. The same push request goes to both servers, and the test waits until both return all 17 lines before asking anything.

## The queries

`queries.txt` is written in-house from the [LogQL documentation](https://grafana.com/docs/loki/latest/query/); nothing is taken from Loki's code or tests. Queries are grouped under the documentation section they exercise (`## log_queries > Line filter expression`). Each line is `range <query>` or `instant <query>`, and `$RUN` stands for the run's label value, so a query only reads that run's data.

To add a query, add a line under its section. A query must use `$RUN`, must not contain `/` (it would split the subtest name), and must not repeat another. `go test ./compatibility/loki/` checks the file without any server.

## How answers are compared

Both answers are normalized the same way, then compared exactly:

- the run label value becomes `RUN`, and timestamps become offsets from the fixture start (`+20s`)
- sample values are compared as numbers (`4` and `4.0` are equal)
- series and streams are sorted by labels, except for `sort` and `sort_desc`, whose order is their answer
- within a stream, lines that share a timestamp are sorted by text: LogQL defines no order among them, and they occur when streams merge (`keep` makes the three checkout pods, which log at the same times, one stream). The order of different timestamps is still compared.
- a non-200 answer is compared by HTTP status only, since error messages differ
- `stats` and other response fields are ignored

Range queries cover one minute before the fixture start to three minutes after, with a 30-second step, `limit=1000` and `direction=backward`. Instant queries are evaluated two minutes after the fixture start.

## The reference Loki's settings

`loki.yaml` turns off one ingestion feature, `discover_log_levels`: with it, Loki detects each line's level and adds it as a `detected_level` label, splitting every stream by level. That isn't LogQL and gigapipe doesn't do it. Measured on this data, leaving it on made 61 of the 67 queries differ.

`discover_service_name` stays at Loki's default: Loki adds a `service_name` label taken from `app`, and gigapipe adds the same label. Measured on this data: with it turned off, 58 of the 67 queries differed; at its default, 23 do.

Everything else is a single-process Loki storing on local disk.

## Settings

`run.sh` always starts its own servers. To compare servers that are already running, skip it and run, from the repository root:

```
GIGAPIPE_URL=http://localhost:3100 LOKI_URL=http://localhost:3101 \
  go test -tags parity -v -run '^TestLogQLParity$' ./compatibility/loki/
```
