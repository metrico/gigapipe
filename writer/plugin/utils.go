package plugin

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/metrico/qryn/writer/ch_wrapper"
	"github.com/metrico/qryn/writer/config"
	controllerv1 "github.com/metrico/qryn/writer/controller"
	apirouterv1 "github.com/metrico/qryn/writer/router"
	"github.com/metrico/qryn/writer/service/registry"
	"github.com/metrico/qryn/writer/utils/logger"
	"github.com/metrico/qryn/writer/utils/numbercache"
	"github.com/metrico/qryn/writer/utils/stat"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var ServiceRegistry registry.ServiceRegistry
var GoCache numbercache.ICache[uint64]

type SetupState struct {
	Version         string
	Type            string
	Shards          int
	SamplesChannels int
	TSChannels      int
	Preforking      bool
	Forks           int
}

func (p *QrynWriterPlugin) humanReadableErrorsFromClickhouse(err error) error {
	if err == nil {
		return nil
	}
	hint := ""
	if strings.Contains(err.Error(), "unexpected packet [21] from server") {
		hint = "You may have misconfigured SSL connection to clickhouse. Please check the database_data.secure option"
	}
	if strings.Contains(err.Error(), "unexpected packet 2") {
		hint = "You may have misconfigured or non-initialized database. Please check database_data.name option. " +
			"It should be an existing and initialized database. In order to initialize the database please run " +
			"\"/cloki-writer -config /path/to/config.json -initialize_db\""
	}
	if hint == "" {
		return err
	}
	return fmt.Errorf("%s. %s", err.Error(), hint)
}

func (p *QrynWriterPlugin) logCHSetup() {
	t := time.NewTicker(time.Hour)
	go func() {
		s := checkSetup(p.ServicesObject.Dbv2Map[0])
		for _, l := range s.ToLogLines() {
			logger.Info(l)
		}
		for range t.C {
			s = checkSetup(p.ServicesObject.Dbv2Map[0])
			for _, l := range s.ToLogLines() {
				logger.Info(l)
			}
		}
	}()
}

func (s SetupState) ToLogLines() []string {
	shards := strconv.FormatInt(int64(s.Shards), 10)
	if s.Shards == 0 {
		shards = "can't retrieve"
	}
	return []string{
		"QRYN-WRITER SETTINGS:",
		"qryn-writer version: " + s.Version,
		"clickhouse setup type: " + s.Type,
		"shards: " + shards,
		"samples channels: " + strconv.FormatInt(int64(s.SamplesChannels), 10),
		"time-series channels: " + strconv.FormatInt(int64(s.TSChannels), 10),
		fmt.Sprintf("preforking: %v", s.Preforking),
		"forks: " + strconv.FormatInt(int64(s.Forks), 10),
	}
}

func checkSetup(conn ch_wrapper.IChClient) SetupState {
	setupType := "single-server"
	if config.Cloki.Setting.DATABASE_DATA[0].ClusterName != "" && config.Cloki.Setting.DATABASE_DATA[0].Cloud {
		setupType = "Distributed + Replicated"
	} else if config.Cloki.Setting.DATABASE_DATA[0].ClusterName != "" {
		setupType = "Distributed"
	} else if config.Cloki.Setting.DATABASE_DATA[0].Cloud {
		setupType = "Replicated"
	}
	shards := 1
	if config.Cloki.Setting.DATABASE_DATA[0].ClusterName != "" {
		shards = getShardsNum(conn, config.Cloki.Setting.DATABASE_DATA[0].ClusterName)
	}
	forks := 1
	if config.Cloki.Setting.HTTP_SETTINGS.Prefork {
		forks = runtime.GOMAXPROCS(0)
	}
	return SetupState{
		Version:         "",
		Type:            setupType,
		Shards:          shards,
		SamplesChannels: config.Cloki.Setting.SYSTEM_SETTINGS.ChannelsSample,
		TSChannels:      config.Cloki.Setting.SYSTEM_SETTINGS.ChannelsTimeSeries,
		Preforking:      config.Cloki.Setting.HTTP_SETTINGS.Prefork,
		Forks:           forks,
	}
}

func getShardsNum(conn ch_wrapper.IChClient, clusterName string) int {
	to, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	rows, err := conn.Query(to, "select count(distinct shard_num) from system.clusters where cluster=$1", clusterName)
	if err != nil {
		logger.Error("[GSN001] Get shards error: ", err)
		return 0
	}
	defer rows.Close()
	var res uint64
	rows.Next()
	err = rows.Scan(&res)
	if err != nil {
		logger.Error("[GSN002] Get shards error: ", err)
		return 0
	}
	return int(res)
}

func (p *QrynWriterPlugin) performV1APIRouting(
	middlewareFactory controllerv1.MiddlewareConfig,
	middlewareTempoFactory controllerv1.MiddlewareConfig,
	router *mux.Router) {
	apirouterv1.RouteInsertDataApis(router, middlewareFactory)
	apirouterv1.RoutePromDataApis(router, middlewareFactory)
	apirouterv1.RouteElasticDataApis(router, middlewareFactory)
	apirouterv1.RouteInsertTempoApis(router, middlewareTempoFactory)
	apirouterv1.RouteProfileDataApis(router, middlewareFactory)
	apirouterv1.RouteMiscApis(router, middlewareFactory)
}

func (p *QrynWriterPlugin) StartPushStat() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop() // Ensure ticker is stopped when function exits

	statCache := make(map[string]prometheus.Gauge)
	getGauge := func(k string) prometheus.Gauge {
		g, ok := statCache[k]
		if !ok {
			g = promauto.NewGauge(prometheus.GaugeOpts{
				Name: stat.SanitizeName(k),
			})
			statCache[k] = g
		}
		return g
	}

	for range ticker.C {
		stats := stat.GetRate()
		stat.ResetRate()
		// Update Prometheus gauges
		if config.Cloki.Setting.PROMETHEUS_CLIENT.Enable {
			for k, v := range stats {
				getGauge(k).Set(float64(v))
			}
		}

	}
}
