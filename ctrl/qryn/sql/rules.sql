## The file is for the ruler's recording/alerting rule groups
## Queries are separated with ";" and one empty string
## APPEND ONLY!!!!!
## Templating tokens: see log.sql
## org_id is intentionally omitted: gigapipe is single-tenant, matching
## time_series/samples_v3 which carry no tenancy column.

CREATE TABLE IF NOT EXISTS {{.DB}}.rules {{.OnCluster}} (
    namespace  String,
    group_name String,
    config     String,
    updated_at DateTime,
    is_valid   UInt8,
    type       String
) ENGINE = {{.ReplacingMergeTree}}(updated_at)
ORDER BY (namespace, group_name, type) {{.CREATE_SETTINGS}};
