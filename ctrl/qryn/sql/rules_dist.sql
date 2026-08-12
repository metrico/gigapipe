## The file is for the ruler's distributed rule-group table
## Queries are separated with ";" and one empty string
## APPEND ONLY!!!!!
## Templating tokens: see log.sql
## Sharded by group_name so all versions of a group co-locate on one shard,
## letting ReplacingMergeTree FINAL deduplicate tombstones correctly. Under
## federation the shard key becomes cityHash64(oid, group_name) so a tenant's
## group versions still co-locate for FINAL dedup.

CREATE TABLE IF NOT EXISTS {{.DB}}.rules_dist {{.OnCluster}} (
    {{.OID_COL}}
    namespace  String,
    group_name String,
    config     String,
    updated_at DateTime,
    is_valid   UInt8,
    type       String
) ENGINE = Distributed('{{.CLUSTER}}', '{{.DB}}', 'rules', cityHash64({{.OID_KEY}}group_name)) {{.DIST_CREATE_SETTINGS}};

## Federation (FEDERATED=1): best-effort oid column on an already-existing rules_dist table.
{{if .Federated}}ALTER TABLE {{.DB}}.rules_dist {{.OnCluster}}
    {{.OID_ADD_COLUMN}}{{end}};
