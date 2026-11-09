-- Statement 1 of a pass: the capped batch as (pass, trace_id, touched), plus a frontier row (trace id {ZERO})
-- holding the batch's newest touch, or the cutoff when it is empty. Tokens:
--   {DB} table prefix; {PASS} pass id; {IDLE_MS}; {MAX_SPANS}; {CURSOR} (the epoch when absent)
--   {REPLICA_DELAY} seconds (0 unreplicated); {REPLICA_SINCE} see replicaSince in pass.go
--   {RUNNING} oldest running insert's start on any replica; {READS} earlier passes' (at, start) reads
--   {RUNNING_INSERTS} the system.processes filter of those inserts
WITH
    -- The scan starts at the cursor, never before the 1-day TTL; without a cursor, 5 minutes back.
    if(toUnixTimestamp64Milli({CURSOR}) <= 0, now64(3) - INTERVAL 5 MINUTE,
       greatest({CURSOR}, now64(3) - INTERVAL 1 DAY)) AS lo,
    -- The cutoff: the least of the idle bound, the replica-delay bound, any running insert's start
    -- (here and on any replica) and replica_cap.
    (
        WITH
            -- With entries to apply: the newest start among reads taken by replica_since, at least lo.
            if(replica_since IS NULL, {RUNNING},
               greatest(lo, arrayMax(arrayMap(x -> x.2, arrayFilter(x -> ifNull(x.1 <= replica_since, 0), {READS}))))) AS replica_cap
        SELECT least(now64(3) - toIntervalMillisecond({IDLE_MS}),
                     now64(3) - toIntervalSecond(if(d > 0, d + 2, 0)) - toIntervalMillisecond(1),
                     now64(3) - toIntervalMillisecond(toInt64(ceil(e * 1000))),
                     {RUNNING},
                     replica_cap)
        FROM (SELECT ifNull({REPLICA_DELAY}, 0) AS d, {REPLICA_SINCE} AS replica_since) AS r
        CROSS JOIN (SELECT max(elapsed) AS e FROM system.processes WHERE {RUNNING_INSERTS}) AS p
    ) AS cut,
-- Traces touched since lo whose newest touch is not before their newest numbering; both tables are
-- range-scanned from lo's minute.
unnumbered AS (
    SELECT trace_id FROM (
        SELECT trace_id, toUnixTimestamp64Milli(inserted_at) AS touched, toUInt64(0) AS numbered FROM {DB}tempo_traces_touched
        WHERE toStartOfMinute(inserted_at) >= toStartOfMinute(lo) AND inserted_at >= lo
        UNION ALL
        SELECT trace_id, toInt64(0), version FROM {DB}tempo_traces_numbered
        WHERE intDiv(version, 60000) >= intDiv(toUnixTimestamp64Milli(lo), 60000) AND version >= toUnixTimestamp64Milli(lo)
    )
    GROUP BY trace_id
    HAVING max(touched) > 0 AND max(touched) >= max(numbered) AND trace_id != {ZERO}
),
-- The newest touch and span count of each unnumbered trace with spans.
sized AS (
    SELECT trace_id, max(t) AS touched, sum(span) AS n FROM (
        SELECT trace_id, inserted_at AS t, toUInt64(0) AS span FROM {DB}tempo_traces_touched
        WHERE toStartOfMinute(inserted_at) >= toStartOfMinute(lo) AND inserted_at >= lo
        UNION ALL
        SELECT trace_id, toDateTime64(0, 3), toUInt64(1) FROM {DB}tempo_traces
    )
    WHERE trace_id IN (SELECT trace_id FROM unnumbered)
    GROUP BY trace_id
    HAVING n > 0
)
-- Oldest first while the running span total stays within the cap; the first trace always.
SELECT {PASS} AS pass, r.1 AS trace_id, r.2 AS touched
FROM (
    SELECT arrayFilter((x, k, i) -> k <= {MAX_SPANS} OR i = 1, a, arrayCumSum(arrayMap(x -> x.3, a)), arrayEnumerate(a)) AS b
    FROM (
        SELECT arraySort(x -> (x.2, x.1), groupArray((trace_id, touched, n))) AS a FROM (
            SELECT trace_id, touched, n FROM sized WHERE touched <= cut
            ORDER BY touched, trace_id LIMIT {MAX_SPANS}
        )
    )
)
ARRAY JOIN arrayPushBack(arrayMap(x -> (x.1, x.2), b), ({ZERO}, if(empty(b), cut, b[-1].2))) AS r
