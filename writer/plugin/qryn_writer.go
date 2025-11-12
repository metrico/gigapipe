package plugin

import (
	"context"
	"net/http"
	"runtime"
	"time"

	"github.com/gorilla/mux"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v4/writer/chwrapper"
	controllerv1 "github.com/metrico/qryn/v4/writer/controller"
	"github.com/metrico/qryn/v4/writer/model"
	patternCtrl "github.com/metrico/qryn/v4/writer/pattern/controller"
	"github.com/metrico/qryn/v4/writer/plugins"
	"github.com/metrico/qryn/v4/writer/service"
	"github.com/metrico/qryn/v4/writer/utils/helpers"
	"github.com/metrico/qryn/v4/writer/utils/logger"
	"github.com/metrico/qryn/v4/writer/watchdog"
	"gopkg.in/go-playground/validator.v9"
)

type ServicesObject struct {
	DatabaseNodeMap []model.DataDatabasesMap
	Dbv2Map         []chwrapper.IChClient
	Dbv3Map         []chwrapper.IChClientFactory
	MainNode        string
}
type QrynWriterPlugin struct {
	Conn           chwrapper.IChClient
	ServicesObject ServicesObject
	Svc            service.IInsertServiceV2
	DBConnWithDSN  chwrapper.IChClient
	DBConnWithXDSN chwrapper.IChClient
	HTTPServer     *http.Server

	logCHSetupTicker *time.Ticker
	logCHSetupDone   chan struct{}
	pushStatDone     chan struct{}
}

var (
	TsSvcs            = make(service.InsertSvcMap)
	SplSvcs           = make(service.InsertSvcMap)
	MtrSvcs           = make(service.InsertSvcMap)
	TempoSamplesSvcs  = make(service.InsertSvcMap)
	TempoTagsSvcs     = make(service.InsertSvcMap)
	ProfileInsertSvcs = make(service.InsertSvcMap)
	PatternInsertSvcs = make(service.InsertSvcMap)
	MetadataSvcs      = make(service.InsertSvcMap)
)

// var servicesObject ServicesObject
// var usageStatsService *usage.TSStats

// Initialize sets up the plugin with the given configuration.
func (p *QrynWriterPlugin) Initialize(config config.ClokiBaseSettingServer) error {
	logger.InitLogger()

	if config.SYSTEM_SETTINGS.CPUMaxProcs == 0 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	} else {
		runtime.GOMAXPROCS(config.SYSTEM_SETTINGS.CPUMaxProcs)
	}

	// TODO: move this all into a separate /registry module and add plugin support to support dynamic database registries
	var err error
	p.ServicesObject.DatabaseNodeMap, p.ServicesObject.Dbv2Map, p.ServicesObject.Dbv3Map = p.getDataDBSession(config)
	p.ServicesObject.MainNode = ""
	for _, node := range config.DATABASE_DATA {
		if p.ServicesObject.MainNode == "" || node.Primary {
			p.ServicesObject.MainNode = node.Node
		}
	}

	p.Conn, err = chwrapper.NewSmartDatabaseAdapter(&config.DATABASE_DATA[0], true)
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
		p.StartLogCHSetup()
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
	router *mux.Router,
) {
	helpers.SetGlobalLimit(config.HTTP_SETTINGS.InputBufferMB * 1024 * 1024)

	config.Validate = validator.New()

	p.performV1APIRouting(middlewareFactory, middlewareTempoFactory, router)
}

// Stop performs cleanup when the plugin is stopped.
func (p *QrynWriterPlugin) Stop() error {
	logger.Info("Stopping QrynWriterPlugin")
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

	p.StopLogCHSetup()
	logger.Info("logCHSetup stopped.")

	p.StopPushStat()
	logger.Info("pushStat stopped.")

	watchdog.Stop()
	logger.Info("writer watchdog stopped.")

	patternCtrl.Stop()
	logger.Info("pattern controller stopped.")

	allServices := []service.InsertSvcMap{
		TsSvcs, SplSvcs, MtrSvcs, TempoSamplesSvcs,
		TempoTagsSvcs, ProfileInsertSvcs, PatternInsertSvcs,
	}
	for _, svcMap := range allServices {
		for _, svc := range svcMap {
			svc.Stop()
		}
	}

	if p.Conn != nil {
		logger.Info("Closing SmartDatabaseAdapter connection")
		if err := p.Conn.Close(); err != nil {
			logger.Error("Failed to close SmartDatabaseAdapter connection:", err)
			return err
		}
		p.Conn = nil
	}

	for _, db := range p.ServicesObject.Dbv2Map {
		if err := db.Close(); err != nil {
			logger.Error("Failed to close database connection:", err)
			return err
		}
	}

	p.ServicesObject.Dbv2Map = nil // Clear references to the connections
	p.ServicesObject.Dbv3Map = nil // Clear references to the connections
	p.Conn = nil
	TsSvcs = make(service.InsertSvcMap)
	SplSvcs = make(service.InsertSvcMap)
	MtrSvcs = make(service.InsertSvcMap)
	TempoSamplesSvcs = make(service.InsertSvcMap)
	TempoTagsSvcs = make(service.InsertSvcMap)
	ProfileInsertSvcs = make(service.InsertSvcMap)
	PatternInsertSvcs = make(service.InsertSvcMap)
	ServiceRegistry.Stop()
	ServiceRegistry = nil
	GoCache.Stop()
	GoCache = nil

	logger.Info("writer successfully cleaned up")
	return nil
}
