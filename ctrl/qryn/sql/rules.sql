## The file is for the ruler's recording/alerting rule groups
## Queries are separated with ";" and one empty string
## APPEND ONLY!!!!!
## Templating tokens: see log.sql
## Single-tenant by default. Under federation (FEDERATED=1) an `oid` tenancy
## column is prepended (no DEFAULT) and joins the sorting key, matching the
## time_series/samples_v3 federated schema. Tokens come from update.go.

CREATE TABLE IF NOT EXISTS {{.DB}}.rules {{.OnCluster}} (
    {{.OID_COL}}
    namespace  String,
    group_name String,
    config     String,
    updated_at DateTime,
    is_valid   UInt8,
    type       String
) ENGINE = {{.ReplacingMergeTree}}(updated_at)
ORDER BY ({{.OID_KEY}}namespace, group_name, type) {{.CREATE_SETTINGS}};

## Federation (FEDERATED=1): best-effort oid column on an already-existing rules table.
{{if .Federated}}ALTER TABLE {{.DB}}.rules {{.OnCluster}}
    {{.OID_ADD_COLUMN}}{{end}};
