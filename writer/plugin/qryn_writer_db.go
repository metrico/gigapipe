package plugin

import (
	"context"
	"fmt"
	"time"
	"unsafe"

	clickhouse_v2 "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v4/writer/chwrapper"
	config2 "github.com/metrico/qryn/v4/writer/config"
	"github.com/metrico/qryn/v4/writer/model"
	patternCtrl "github.com/metrico/qryn/v4/writer/pattern/controller"
	"github.com/metrico/qryn/v4/writer/service"
	"github.com/metrico/qryn/v4/writer/service/insert"
	"github.com/metrico/qryn/v4/writer/service/registry"
	"github.com/metrico/qryn/v4/writer/utils/logger"
	"github.com/metrico/qryn/v4/writer/utils/numbercache"
	"github.com/metrico/qryn/v4/writer/watchdog"
)

var MainNode string

const (
	ClustModeSingle      = 1
	ClustModeCloud       = 2
	ClustModeDistributed = 4
	ClustModeStats       = 8
)

func (p *QrynWriterPlugin) getDataDBSession(config config.ClokiBaseSettingServer) ([]model.DataDatabasesMap, []chwrapper.IChClient, []chwrapper.IChClientFactory) {
	dbNodeMap := []model.DataDatabasesMap{}
	// dbv2Map := []clickhouse_v2.Conn{}
	dbv2Map := []chwrapper.IChClient{}
	// dbv3Map := []service.IChClientFactory{}
	dbv3Map := []chwrapper.IChClientFactory{}
	// Rlogs
	if logger.RLogs != nil {
		clickhouse_v2.WithLogs(func(log *clickhouse_v2.Log) {
			logger.RLogs.Write([]byte(log.Text))
		})
	}

	for _, dbObject := range config.DATABASE_DATA {
		connv2, err := chwrapper.NewSmartDatabaseAdapter(&dbObject, true)
		if err != nil {
			err = p.humanReadableErrorsFromClickhouse(err)
			logger.Error(fmt.Sprintf("couldn't make connection to [Host: %s, Node: %s, Port: %d]: \n", dbObject.Host, dbObject.Node, dbObject.Port), err)
			continue
		}

		dbv2Map = append(dbv2Map, connv2)

		dbv3Map = append(dbv3Map, func() (chwrapper.IChClient, error) {
			connV3, err := chwrapper.NewSmartDatabaseAdapter(&dbObject, true)
			return connV3, err
		})
		// connV3, err := ch_wrapper.NewSmartDatabaseAdapter(&dbObject, true)
		// dbv3Map = append(dbv3Map, connV3)

		dbNodeMap = append(dbNodeMap,
			model.DataDatabasesMap{ClokiBaseDataBase: dbObject})

		logger.Info("----------------------------------- ")
		logger.Info("*** Database Session created *** ")
		logger.Info("----------------------------------- ")
	}

	return dbNodeMap, dbv2Map, dbv3Map
}

func healthCheck(conn chwrapper.IChClient, isDistributed bool) {
	tablesToCheck := []string{
		"time_series", "samples_v3", "settings",
		"tempo_traces", "tempo_traces_attrs_gin",
	}
	distTablesToCheck := []string{
		"samples_v3_dist", " time_series_dist",
		"tempo_traces_dist", "tempo_traces_attrs_gin_dist",
	}
	checkTable := func(table string) error {
		query := fmt.Sprintf("SELECT 1 FROM %s LIMIT 1", table)
		to, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()
		rows, err := conn.Query(to, query)
		if err != nil {
			return err
		}
		defer rows.Close()
		return nil
	}
	for _, table := range tablesToCheck {
		logger.Info("Checking ", table, " table")
		err := checkTable(table)
		if err != nil {
			logger.Error(err)
			panic(err)
		}
		logger.Info("Check ", table, " ok")
	}
	if isDistributed {
		for _, table := range distTablesToCheck {
			logger.Info("Checking ", table, " table")
			err := checkTable(table)
			if err != nil {
				logger.Error(err)
				panic(err)
			}
			logger.Info("Check ", table, " ok")
		}
	}
}

func (p *QrynWriterPlugin) CreateStaticServiceRegistry(config config.ClokiBaseSettingServer) {
	databasesNodeHashMap := make(map[string]*model.DataDatabasesMap)
	for _, node := range p.ServicesObject.DatabaseNodeMap {
		databasesNodeHashMap[node.Node] = &node
	}

	for i, node := range p.ServicesObject.DatabaseNodeMap {
		if MainNode == "" || node.Primary {
			MainNode = node.Node
		}

		_node := node.Node

		TsSvcs[node.Node] = insert.NewTimeSeriesInsertService(model.InsertServiceOpts{
			Session:     p.ServicesObject.Dbv3Map[i],
			Node:        &node,
			Interval:    time.Millisecond * time.Duration(config.SYSTEM_SETTINGS.DBTimer*1000),
			ParallelNum: config.SYSTEM_SETTINGS.ChannelsTimeSeries,
			AsyncInsert: node.AsyncInsert,
		})
		TsSvcs[node.Node].Init()

		go TsSvcs[node.Node].Run()

		SplSvcs[node.Node] = insert.NewSamplesInsertService(model.InsertServiceOpts{
			Session:        p.ServicesObject.Dbv3Map[i],
			Node:           &node,
			Interval:       time.Millisecond * time.Duration(config.SYSTEM_SETTINGS.DBTimer*1000),
			ParallelNum:    config.SYSTEM_SETTINGS.ChannelsSample,
			AsyncInsert:    node.AsyncInsert,
			MaxQueueSize:   int64(config.SYSTEM_SETTINGS.DBBulk),
			OnBeforeInsert: func() { TsSvcs[_node].PlanFlush() },
		})
		SplSvcs[node.Node].Init()
		go SplSvcs[node.Node].Run()

		MtrSvcs[node.Node] = insert.NewMetricsInsertService(model.InsertServiceOpts{
			Session:        p.ServicesObject.Dbv3Map[i],
			Node:           &node,
			Interval:       time.Millisecond * time.Duration(config.SYSTEM_SETTINGS.DBTimer*1000),
			ParallelNum:    config.SYSTEM_SETTINGS.ChannelsSample,
			AsyncInsert:    node.AsyncInsert,
			MaxQueueSize:   int64(config.SYSTEM_SETTINGS.DBBulk),
			OnBeforeInsert: func() { TsSvcs[_node].PlanFlush() },
		})
		MtrSvcs[node.Node].Init()
		go MtrSvcs[node.Node].Run()

		TempoSamplesSvcs[node.Node] = insert.NewTempoSamplesInsertService(model.InsertServiceOpts{
			Session:        p.ServicesObject.Dbv3Map[i],
			Node:           &node,
			Interval:       time.Millisecond * time.Duration(config.SYSTEM_SETTINGS.DBTimer*1000),
			ParallelNum:    config.SYSTEM_SETTINGS.ChannelsSample,
			AsyncInsert:    node.AsyncInsert,
			MaxQueueSize:   int64(config.SYSTEM_SETTINGS.DBBulk),
			OnBeforeInsert: func() { TempoTagsSvcs[_node].PlanFlush() },
		})
		TempoSamplesSvcs[node.Node].Init()
		go TempoSamplesSvcs[node.Node].Run()

		TempoTagsSvcs[node.Node] = insert.NewTempoTagsInsertService(model.InsertServiceOpts{
			Session:        p.ServicesObject.Dbv3Map[i],
			Node:           &node,
			Interval:       time.Millisecond * time.Duration(config.SYSTEM_SETTINGS.DBTimer*1000),
			ParallelNum:    config.SYSTEM_SETTINGS.ChannelsSample,
			AsyncInsert:    node.AsyncInsert,
			MaxQueueSize:   int64(config.SYSTEM_SETTINGS.DBBulk),
			OnBeforeInsert: func() { TempoSamplesSvcs[_node].PlanFlush() },
		})
		TempoTagsSvcs[node.Node].Init()
		go TempoTagsSvcs[node.Node].Run()
		ProfileInsertSvcs[node.Node] = insert.NewProfileSamplesInsertService(model.InsertServiceOpts{
			Session:     p.ServicesObject.Dbv3Map[i],
			Node:        &node,
			Interval:    time.Millisecond * time.Duration(config.SYSTEM_SETTINGS.DBTimer*1000),
			ParallelNum: config.SYSTEM_SETTINGS.ChannelsSample,
			AsyncInsert: node.AsyncInsert,
		})
		ProfileInsertSvcs[node.Node].Init()
		go ProfileInsertSvcs[node.Node].Run()

		PatternInsertSvcs[node.Node] = insert.NewPatternInsertService(model.InsertServiceOpts{
			Session:        p.ServicesObject.Dbv3Map[i],
			Node:           &node,
			Interval:       time.Millisecond * time.Duration(config.SYSTEM_SETTINGS.DBTimer*1000),
			ParallelNum:    config.SYSTEM_SETTINGS.ChannelsSample,
			AsyncInsert:    node.AsyncInsert,
			MaxQueueSize:   int64(config.SYSTEM_SETTINGS.DBBulk),
			OnBeforeInsert: func() { TempoSamplesSvcs[_node].PlanFlush() },
		})
		PatternInsertSvcs[node.Node].Init()
		go PatternInsertSvcs[node.Node].Run()

		table := "qryn_fingerprints"
		if node.ClusterName != "" {
			table += "_dist"
		}
	}

	ServiceRegistry = registry.NewStaticServiceRegistry(registry.StaticServiceRegistryOpts{
		TimeSeriesSvcs:    TsSvcs,
		SamplesSvcs:       SplSvcs,
		MetricSvcs:        MtrSvcs,
		TempoSamplesSvcs:  TempoSamplesSvcs,
		TempoTagsSvcs:     TempoTagsSvcs,
		ProfileInsertSvcs: ProfileInsertSvcs,
		PatternInsertSvcs: PatternInsertSvcs,
	})

	GoCache = numbercache.NewCache(time.Minute*30, func(val uint64) []byte {
		return unsafe.Slice((*byte)(unsafe.Pointer(&val)), 8)
	}, databasesNodeHashMap)

	watchdog.Init([]service.InsertSvcMap{
		TsSvcs,
		SplSvcs,
		MtrSvcs,
		TempoSamplesSvcs,
		TempoTagsSvcs,
		ProfileInsertSvcs,
		PatternInsertSvcs,
	})

	if config2.Cloki.Setting.DRILLDOWN_SETTINGS.LogDrilldown {
		patternCtrl.Init(patternCtrl.InitOpts{
			Service:   ServiceRegistry,
			Conns:     p.ServicesObject.Dbv3Map[0],
			IsCluster: p.ServicesObject.DatabaseNodeMap[0].ClusterName != "",
		})
	}

	// Run Prometheus Scaper
	// go promscrape.RunPrometheusScraper(goCache, TsSvcs, MtrSvcs)
}
