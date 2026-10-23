-- Statement 2 of a pass: the numbering of the batch, in tempo_traces_nested_set column order.
-- Tokens: {DB} table prefix, {PASS} pass id.
WITH RECURSIVE
-- One node per span id: the earliest copy places it.
edges AS (
    SELECT trace_id, span_id,
           argMin(toFixedString(substring(parent_id, 1, 8), 8), timestamp_ns) AS parent,
           min(timestamp_ns) AS ts
    FROM {DB}tempo_traces
    WHERE trace_id IN (SELECT trace_id FROM {DB}tempo_traces_nested_set_batch WHERE pass = {PASS})
    GROUP BY trace_id, span_id
),
links AS (
    SELECT e.trace_id AS trace_id, e.span_id AS span_id, e.parent AS parent, e.ts AS ts,
           toUInt8(e.parent = toFixedString('', 8)) AS is_root,
           toUInt8(e.parent != toFixedString('', 8) AND ifNull(p.found, 0) = 0) AS dangling
    FROM edges AS e
    LEFT JOIN (SELECT trace_id, span_id, toUInt8(1) AS found FROM edges) AS p
      ON p.trace_id = e.trace_id AND p.span_id = e.parent
),
-- 2^R >= the largest trace's span count, so a span unresolved after R rounds is on a cycle.
rounds AS (
    SELECT toUInt32(greatest(1, ceil(log2(max(n))))) AS r
    FROM (SELECT count() AS n FROM edges GROUP BY trace_id)
),
-- anc = 2^k-th ancestor (zero past the top), cut = a missing parent on the way, cnt = descendants-or-self
-- closer than 2^k. Reachable after R rounds: cut = 0 and anc zero.
up AS (
    SELECT trace_id, span_id AS node,
           if(is_root = 1 OR dangling = 1, toFixedString('', 8), parent) AS anc,
           dangling AS cut, toUInt64(1) AS cnt, toUInt32(0) AS k
    FROM links
  UNION ALL
    SELECT x.trace_id, x.node,
           if(x.anc = toFixedString('', 8), x.anc, ifNull(y.anc, toFixedString('', 8))) AS anc,
           toUInt8(x.cut = 1 OR (x.anc != toFixedString('', 8) AND ifNull(y.cut, 0) = 1)) AS cut,
           x.cnt + ifNull(f.s, 0) AS cnt,
           toUInt32(x.k + 1) AS k
    FROM up AS x
    LEFT JOIN up AS y ON y.trace_id = x.trace_id AND y.node = x.anc
    LEFT JOIN (
        SELECT trace_id, anc AS node, sum(cnt) AS s FROM up
        WHERE anc != toFixedString('', 8)
        GROUP BY trace_id, anc
    ) AS f ON f.trace_id = x.trace_id AND f.node = x.node
    WHERE x.k < (SELECT r FROM rounds)
),
-- d = Left(span) - Left(parent) = 1 + 2 * the sizes of earlier siblings (or roots).
sized AS (
    SELECT l.trace_id AS trace_id, l.span_id AS node, l.parent AS parent, l.is_root AS is_root,
           u.cnt AS size,
           toInt64(1 + 2 * (sum(u.cnt) OVER (PARTITION BY l.trace_id, l.parent ORDER BY l.ts, l.span_id
                                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - u.cnt)) AS d
    FROM links AS l
    INNER JOIN (SELECT trace_id, node, cnt FROM up
                WHERE k = (SELECT r FROM rounds) AND cut = 0 AND anc = toFixedString('', 8)) AS u
      ON u.trace_id = l.trace_id AND u.node = l.span_id
),
-- acc = the sum of d over the span and its ancestors closer than 2^k.
down AS (
    SELECT trace_id, node, if(is_root = 1, toFixedString('', 8), parent) AS anc,
           d AS acc, toUInt32(0) AS k
    FROM sized
  UNION ALL
    SELECT x.trace_id, x.node,
           if(x.anc = toFixedString('', 8), x.anc, ifNull(y.anc, toFixedString('', 8))) AS anc,
           x.acc + if(x.anc = toFixedString('', 8), 0, ifNull(y.acc, 0)) AS acc,
           toUInt32(x.k + 1) AS k
    FROM down AS x
    LEFT JOIN down AS y ON y.trace_id = x.trace_id AND y.node = x.anc
    WHERE x.k < (SELECT r FROM rounds)
),
numbered AS (
    SELECT s.trace_id AS trace_id, s.node AS span_id,
           w.acc AS nested_left,
           w.acc + 2 * toInt64(s.size) - 1 AS nested_right,
           if(s.is_root = 1, toInt64(-1), w.acc - s.d) AS nested_parent
    FROM sized AS s
    INNER JOIN (SELECT trace_id, node, acc FROM down WHERE k = (SELECT r FROM rounds)) AS w
      ON w.trace_id = s.trace_id AND w.node = s.node
)
SELECT l.trace_id AS trace_id, l.span_id AS span_id,
       toDate(FROM_UNIXTIME(intDiv(l.ts, 1000000000))) AS date,
       toInt32(ifNull(n.nested_left, 0)) AS left,
       toInt32(ifNull(n.nested_right, 0)) AS right,
       toInt32(ifNull(n.nested_parent, 0)) AS parent,
       toUInt64(toUnixTimestamp64Milli(now64(3))) AS version
FROM links AS l
LEFT JOIN numbered AS n ON n.trace_id = l.trace_id AND n.span_id = l.span_id
