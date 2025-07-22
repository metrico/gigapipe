package plugins

type TableNamesPlugin func() map[string]string

var tableNamesPlugin *TableNamesPlugin

func RegisterTableNamesPlugin(name string, plugin TableNamesPlugin) {
	tableNamesPlugin = &plugin
}

func GetTableNamesPlugin() *TableNamesPlugin {
	return tableNamesPlugin
}
