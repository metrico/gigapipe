package plugin

import (
	"context"
	"fmt"
	clickhouse_v2 "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/writer/ch_wrapper"
	config2 "github.com/metrico/qryn/writer/config"
	"github.com/metrico/qryn/writer/model"
	patternCtrl "github.com/metrico/qryn/writer/pattern/controller"

	"github.com/metrico/qryn/writer/service"
	"github.com/metrico/qryn/writer/service/impl"
	"github.com/metrico/qryn/writer/service/registry"
	"github.com/metrico/qryn/writer/utils/logger"
	"github.com/metrico/qryn/writer/utils/numbercache"
	"github.com/metrico/qryn/writer/watchdog"
	"time"
	"unsafe"
)

var MainNode string

const (
	ClustModeSingle      = 1
	ClustModeCloud       = 2
	ClustModeDistributed = 4
	ClustModeStats       = 8
)

func initDB(dbObject *config.ClokiBaseDataBase) error {
	ctx := context.Background()
	//client, err := adapter.NewClient(ctx, dbObject, false)
	client, err := ch_wrapper.NewSmartDatabaseAdapter(dbObject, true)
	if err != nil {
		return err
	}
	if dbObject.Name != "" && dbObject.Name != "default" {
		engine := ""
		if dbObject.Cloud {
			engine = "ENGINE = Atomic"
		}
		onCluster := ""
		if dbObject.ClusterName != "" {
			onCluster = fmt.Sprintf("ON CLUSTER `%s`", dbObject.ClusterName)
		}
		err = client.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` %s "+engine, dbObject.Name, onCluster))
		if err != nil {
			err1 := err
			logger.Info("Database creation error. Retrying without the engine", err1)
			err = client.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` %s", dbObject.Name, onCluster))
			if err != nil {
				return fmt.Errorf("database creation errors: %s; %s", err1.Error(), err.Error())
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *QrynWriterPlugin) getDataDBSession(config config.ClokiBaseSettingServer) ([]model.DataDatabasesMap, []ch_wrapper.IChClient, []ch_wrapper.IChClientFactory) {

	dbNodeMap := []model.DataDatabasesMap{}
	//dbv2Map := []clickhouse_v2.Conn{}
	dbv2Map := []ch_wrapper.IChClient{}
	//dbv3Map := []service.IChClientFactory{}
	dbv3Map := []ch_wrapper.IChClientFactory{}
	// Rlogs
	if logger.RLogs != nil {
		clickhouse_v2.WithLogs(func(log *clickhouse_v2.Log) {
			logger.RLogs.Write([]byte(log.Text))
		})
	}

	for _, dbObject := range config.DATABASE_DATA {
		connv2, err := ch_wrapper.NewSmartDatabaseAdapter(&dbObject, true)
		if err != nil {
			err = p.humanReadableErrorsFromClickhouse(err)
			logger.Error(fmt.Sprintf("couldn't make connection to [Host: %s, Node: %s, Port: %d]: \n", dbObject.Host, dbObject.Node, dbObject.Port), err)
			continue
		}

		dbv2Map = append(dbv2Map, connv2)

		dbv3Map = append(dbv3Map, func() (ch_wrapper.IChClient, error) {
			connV3, err := ch_wrapper.NewSmartDatabaseAdapter(&dbObject, true)
			return connV3, err
		})
		//connV3, err := ch_wrapper.NewSmartDatabaseAdapter(&dbObject, true)
		//dbv3Map = append(dbv3Map, connV3)

		dbNodeMap = append(dbNodeMap,
			model.DataDatabasesMap{ClokiBaseDataBase: dbObject})

		logger.Info("----------------------------------- ")
		logger.Info("*** Database Session created *** ")
		logger.Info("----------------------------------- ")
	}

	return dbNodeMap, dbv2Map, dbv3Map
}

func healthCheck(conn ch_wrapper.IChClient, isDistributed bool) {
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
		to, _ := context.WithTimeout(context.Background(), time.Second*30)
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

func checkAll(base []config.ClokiBaseDataBase) error {
	for _, dbObject := range base {
		logger.Info(fmt.Sprintf("Checking %s:%d/%s", dbObject.Host, dbObject.Port, dbObject.Name))
		mode := ClustModeSingle
		if dbObject.Cloud {
			mode = ClustModeCloud
		}
		if dbObject.ClusterName != "" {
			mode = mode | ClustModeDistributed
		}
		err := func() error {
			//client, err := adapter.NewClient(context.Background(), &dbObject, true)
			client, err := ch_wrapper.NewSmartDatabaseAdapter(&dbObject, true)
			if err != nil {
				return err
			}
			defer func(client ch_wrapper.IChClient) {
				err := client.Close()
				if err != nil {
					logger.Error("Error closing client", err)
				}
			}(client)
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
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

		TsSvcs[node.Node] = impl.NewTimeSeriesInsertService(model.InsertServiceOpts{
			Session:     p.ServicesObject.Dbv3Map[i],
			Node:        &node,
			Interval:    time.Millisecond * time.Duration(config.SYSTEM_SETTINGS.DBTimer*1000),
			ParallelNum: config.SYSTEM_SETTINGS.ChannelsTimeSeries,
			AsyncInsert: node.AsyncInsert,
		})
		TsSvcs[node.Node].Init()

		go TsSvcs[node.Node].Run()

		SplSvcs[node.Node] = impl.NewSamplesInsertService(model.InsertServiceOpts{
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

		MtrSvcs[node.Node] = impl.NewMetricsInsertService(model.InsertServiceOpts{
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

		TempoSamplesSvcs[node.Node] = impl.NewTempoSamplesInsertService(model.InsertServiceOpts{
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

		TempoTagsSvcs[node.Node] = impl.NewTempoTagsInsertService(model.InsertServiceOpts{
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
		ProfileInsertSvcs[node.Node] = impl.NewProfileSamplesInsertService(model.InsertServiceOpts{
			Session:     p.ServicesObject.Dbv3Map[i],
			Node:        &node,
			Interval:    time.Millisecond * time.Duration(config.SYSTEM_SETTINGS.DBTimer*1000),
			ParallelNum: config.SYSTEM_SETTINGS.ChannelsSample,
			AsyncInsert: node.AsyncInsert,
		})
		ProfileInsertSvcs[node.Node].Init()
		go ProfileInsertSvcs[node.Node].Run()

		PatternInsertSvcs[node.Node] = impl.NewPatternInsertService(model.InsertServiceOpts{
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

	GoCache = numbercache.NewCache[uint64](time.Minute*30, func(val uint64) []byte {
		return unsafe.Slice((*byte)(unsafe.Pointer(&val)), 8)
	}, databasesNodeHashMap)

	watchdog.Init([]service.InsertSvcMap{
		TsSvcs,
		SplSvcs,
		MtrSvcs,
		TempoSamplesSvcs,
		TempoTagsSvcs,
		ProfileInsertSvcs,
	})

	if config2.Cloki.Setting.DRILLDOWN_SETTINGS.LogDrilldown {
		patternCtrl.Init(patternCtrl.InitOpts{
			Service:   ServiceRegistry,
			Conns:     p.ServicesObject.Dbv3Map[0],
			IsCluster: p.ServicesObject.DatabaseNodeMap[0].ClusterName != "",
		})
	}

	//Run Prometheus Scaper
	//go promscrape.RunPrometheusScraper(goCache, TsSvcs, MtrSvcs)

}
