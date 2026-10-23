-- Statement 1 of a pass: the capped batch of trace ids as (pass, trace_id).
-- Tokens: {DB} table prefix, {PASS} pass id, {IDLE_MS}, {MAX_SPANS}, {REPLICA_DELAY} (seconds, 0 unreplicated).
WITH
-- Traces touched at least the idle period ago and not before their newest numbering.
-- On a lagging replica they must also be touched before now - delay - 2s.
-- Touches past the 1-day TTL are ignored.
eligible AS (
    SELECT trace_id FROM (
        SELECT trace_id, toUnixTimestamp64Milli(inserted_at) AS touched, toInt64(0) AS numbered FROM {DB}tempo_traces_touched
        WHERE inserted_at > now64(3) - INTERVAL 1 DAY
        UNION ALL
        SELECT trace_id, toInt64(0), toInt64(version) FROM {DB}tempo_traces_numbered
    )
    GROUP BY trace_id
    HAVING max(touched) <= toUnixTimestamp64Milli(now64(3) - toIntervalMillisecond({IDLE_MS}))
       AND max(touched) < toUnixTimestamp64Milli(now64(3) - toIntervalSecond((SELECT if(d > 0, d + 2, 0) FROM (SELECT {REPLICA_DELAY} AS d))))
       AND max(touched) >= max(numbered)
),
-- The newest touch and span count of each eligible trace; eligible is referenced only here.
sized AS (
    SELECT trace_id, max(touched) AS touched, sum(span) AS n FROM (
        SELECT trace_id, inserted_at AS touched, toUInt64(0) AS span FROM {DB}tempo_traces_touched
        UNION ALL
        SELECT trace_id, toDateTime64(0, 3), toUInt64(1) FROM {DB}tempo_traces
    )
    WHERE trace_id IN (SELECT trace_id FROM eligible)
    GROUP BY trace_id
    HAVING n > 0
),
-- Oldest first while the running span total stays within the cap; the first trace always.
cand AS (
    SELECT trace_id FROM (
        SELECT trace_id, n,
               sum(n) OVER (ORDER BY touched, trace_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum
        FROM sized
    ) WHERE cum <= {MAX_SPANS} OR cum = n
)
SELECT {PASS} AS pass, trace_id FROM cand
