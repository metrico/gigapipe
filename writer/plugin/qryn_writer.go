package plugin

import (
	"context"

	"github.com/gorilla/mux"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/writer/ch_wrapper"
	controllerv1 "github.com/metrico/qryn/writer/controller"
	"github.com/metrico/qryn/writer/model"
	"github.com/metrico/qryn/writer/plugins"
	"github.com/metrico/qryn/writer/service"

	//config3 "github.com/metrico/qryn/writer/usagecounter/config"
	"net/http"
	"runtime"
	"time"

	"github.com/metrico/qryn/writer/utils/helpers"
	"github.com/metrico/qryn/writer/utils/logger"
	"gopkg.in/go-playground/validator.v9"
	//	"os"
)

type ServicesObject struct {
	DatabaseNodeMap []model.DataDatabasesMap
	Dbv2Map         []ch_wrapper.IChClient
	Dbv3Map         []ch_wrapper.IChClientFactory
	MainNode        string
}
type QrynWriterPlugin struct {
	Conn           ch_wrapper.IChClient
	ServicesObject ServicesObject
	Svc            service.IInsertServiceV2
	DBConnWithDSN  ch_wrapper.IChClient
	DBConnWithXDSN ch_wrapper.IChClient
	HTTPServer     *http.Server
}

var TsSvcs = make(service.InsertSvcMap)
var SplSvcs = make(service.InsertSvcMap)
var MtrSvcs = make(service.InsertSvcMap)
var TempoSamplesSvcs = make(service.InsertSvcMap)
var TempoTagsSvcs = make(service.InsertSvcMap)
var ProfileInsertSvcs = make(service.InsertSvcMap)
var PatternInsertSvcs = make(service.InsertSvcMap)

//var servicesObject ServicesObject
//var usageStatsService *usage.TSStats

// Initialize sets up the plugin with the given configuration.
func (p *QrynWriterPlugin) Initialize(config config.ClokiBaseSettingServer) error {

	logger.InitLogger()

	if config.SYSTEM_SETTINGS.CPUMaxProcs == 0 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	} else {
		runtime.GOMAXPROCS(config.SYSTEM_SETTINGS.CPUMaxProcs)
	}

	//TODO: move this all into a separate /registry module and add plugin support to support dynamic database registries
	var err error
	p.ServicesObject.DatabaseNodeMap, p.ServicesObject.Dbv2Map, p.ServicesObject.Dbv3Map = p.getDataDBSession(config)
	p.ServicesObject.MainNode = ""
	for _, node := range config.DATABASE_DATA {
		if p.ServicesObject.MainNode == "" || node.Primary {
			p.ServicesObject.MainNode = node.Node
		}
	}

	p.Conn, err = ch_wrapper.NewSmartDatabaseAdapter(&config.DATABASE_DATA[0], true)
	if err != nil {
		panic(err)
	}
	//// maintain databases
	//plugins.RegisterDatabaseSessionPlugin(p.getDataDBSession)
	//plugins.RegisterHealthCheckPlugin(healthCheck)
	healthCheckPlugin := plugins.GetHealthCheckPlugin()
	for i, dbObject := range config.DATABASE_DATA {
		isDistributed := dbObject.ClusterName != ""
		conn := p.ServicesObject.Dbv2Map[i]
		if healthCheckPlugin != nil {
			(*healthCheckPlugin)(conn, isDistributed)
		} else {
			healthCheck(conn, isDistributed)
		}

	}
	//for i, dbObject := range config.DATABASE_DATA {
	//	//TODO: move this into the /registry and with the plugin support
	//	healthCheck(p.ServicesObject.Dbv2Map[i], dbObject.ClusterName != "")
	//}

	if !config.HTTP_SETTINGS.Prefork {
		p.logCHSetup()
	}

	poolSize := (config.SYSTEM_SETTINGS.ChannelsTimeSeries*2*2+
		config.SYSTEM_SETTINGS.ChannelsSample*2*11)*
		len(config.DATABASE_DATA) + 20

	if config.SYSTEM_SETTINGS.DynamicDatabases {
		poolSize = 1000
	}
	logger.Info("PoolSize: ", poolSize)
	service.CreateColPools(int32(poolSize))

	return nil

}

// RegisterRoutes registers the plugin routes with the provided HTTP ServeMux.
func (p *QrynWriterPlugin) RegisterRoutes(config config.ClokiBaseSettingServer,
	middlewareFactory controllerv1.MiddlewareConfig,
	middlewareTempoFactory controllerv1.MiddlewareConfig,
	router *mux.Router) {
	helpers.SetGlobalLimit(config.HTTP_SETTINGS.InputBufferMB * 1024 * 1024)

	config.Validate = validator.New()

	p.performV1APIRouting(middlewareFactory, middlewareTempoFactory, router)
}

// Stop performs cleanup when the plugin is stopped.
func (p *QrynWriterPlugin) Stop() error {

	logger.Info("Stopping QrynWriterPlugin")

	// Stop the HTTP server
	if p.HTTPServer != nil {
		logger.Info("Shutting down HTTP server")
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
		defer cancel()
		if err := p.HTTPServer.Shutdown(ctx); err != nil {
			logger.Error("Failed to gracefully shut down HTTP server:", err)
			return err
		}
		p.HTTPServer = nil
		logger.Info("HTTP server successfully stopped")
	}

	// Close all database connections in servicesObject
	for _, db := range p.ServicesObject.Dbv2Map {
		if err := db.Close(); err != nil {
			logger.Error("Failed to close database connection:", err)
			return err
		}
	}

	p.ServicesObject.Dbv2Map = nil // Clear references to the connections

	p.ServicesObject.Dbv3Map = nil // Clear references to the connections

	if p.Conn != nil {
		logger.Info("Closing SmartDatabaseAdapter connection")
		if err := p.Conn.Close(); err != nil { // Assuming `Close` is a valid method
			logger.Error("Failed to close SmartDatabaseAdapter connection:", err)
			return err
		}
		p.Conn = nil // Clear the reference
	}

	mainNode := ""
	for _, node := range p.ServicesObject.DatabaseNodeMap {
		if mainNode == "" || node.Primary {
			mainNode = node.Node
		}

		TsSvcs[node.Node].Stop()
		SplSvcs[node.Node].Stop()
		MtrSvcs[node.Node].Stop()
		TempoSamplesSvcs[node.Node].Stop()
		TempoTagsSvcs[node.Node].Stop()
		ProfileInsertSvcs[node.Node].Stop()
	}

	//config3.CountService.Stop()
	//if serviceRegistry != nil {
	//	serviceRegistry.Stop()
	//	serviceRegistry = nil
	//}
	logger.Info("All resources successfully cleaned up")
	return nil
}
