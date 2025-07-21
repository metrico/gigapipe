package model

import "github.com/metrico/cloki-config/config"

type DataDatabasesMap struct {
	Config  *config.ClokiBaseDataBase
	DSN     string `json:"dsn"`
	Session ISqlxDB
}

type ConfigDatabasesMap struct {
	Value           string   `json:"value"`
	Name            string   `json:"name"`
	Node            string   `json:"node"`
	Host            string   `json:"host"`
	Primary         bool     `json:"primary"`
	Online          bool     `json:"online"`
	URL             string   `json:"url"`
	ProtectedTables []string `json:"-"`
	SkipTables      []string `json:"-"`
}

type ConfigURLNode struct {
	Name    string `json:"name"`
	URL     string `json:"url"`
	Primary bool   `json:"primary"`
}
