## The file is for the ruler's distributed rule-group table
## Queries are separated with ";" and one empty string
## APPEND ONLY!!!!!
## Templating tokens: see log.sql
## Sharded by group_name so all versions of a group co-locate on one shard,
## letting ReplacingMergeTree FINAL deduplicate tombstones correctly.

CREATE TABLE IF NOT EXISTS {{.DB}}.rules_dist {{.OnCluster}} (
    namespace  String,
    group_name String,
    config     String,
    updated_at DateTime,
    is_valid   UInt8,
    type       String
) ENGINE = Distributed('{{.CLUSTER}}', '{{.DB}}', 'rules', cityHash64(group_name)) {{.DIST_CREATE_SETTINGS}};
