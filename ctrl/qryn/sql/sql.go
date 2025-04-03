package sql

import _ "embed"

//go:embed log.sql
var LogScript string

//go:embed log_dist.sql
var LogDistScript string

//go:embed traces.sql
var TracesScript string

//go:embed traces_dist.sql
var TracesDistScript string

//go:embed profiles.sql
var ProfilesScript string

//go:embed profiles_dist.sql
var ProfilesDistScript string
