package plugins

import (
	"context"
	"errors"
	"net/http"

	"github.com/metrico/qryn/reader/logql/parser"
	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	"github.com/metrico/qryn/reader/model"
)

var ErrPluginNotApplicable = errors.New("plugin not applicable")

type LogQLTranspilerPlugin interface {
	Plan(script *parser.LogQLScript) (shared.RequestProcessorChain, error)
}

var logQLTranspilerPlugins []LogQLTranspilerPlugin

func RegisterLogQLPlannerPlugin(name string, plugin LogQLTranspilerPlugin) {
	logQLTranspilerPlugins = append(logQLTranspilerPlugins, plugin)
}

func GetLogQLPlannerPlugins() []LogQLTranspilerPlugin {
	return logQLTranspilerPlugins
}

type PreRequestPlugin func(ctx context.Context, req *http.Request) (context.Context, error)

var preRequestPlugins []PreRequestPlugin

func RegisterPreRequestPlugin(name string, plugin PreRequestPlugin) {
	preRequestPlugins = append(preRequestPlugins, plugin)
}

func GetPreRequestPlugins() []PreRequestPlugin {
	return preRequestPlugins
}

type PreWSRequestPlugin func(ctx context.Context, req *http.Request) (context.Context, error)

var preWSRequestPlugins []PreWSRequestPlugin

func RegisterPreWSRequestPlugin(name string, plugin PreWSRequestPlugin) {
	preWSRequestPlugins = append(preWSRequestPlugins, plugin)
}

func GetPreWSRequestPlugins() []PreWSRequestPlugin {
	return preWSRequestPlugins
}

type DatabaseRegistryPlugin func() model.IDBRegistry

var databaseRegistryPlugin *DatabaseRegistryPlugin

func RegisterDatabaseRegistryPlugin(plugin DatabaseRegistryPlugin) {
	databaseRegistryPlugin = &plugin
}

func GetDatabaseRegistryPlugin() *DatabaseRegistryPlugin {
	return databaseRegistryPlugin
}
