package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/grafana/pyroscope-go"
	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/ctrl"
	"github.com/metrico/qryn/reader"
	"github.com/metrico/qryn/reader/utils/logger"
	"github.com/metrico/qryn/reader/utils/middleware"
	"github.com/metrico/qryn/shared/commonroutes"
	"github.com/metrico/qryn/view"
	"github.com/metrico/qryn/writer"
)

var appFlags CommandLineFlags

// params for Flags
type CommandLineFlags struct {
	InitializeDB    *bool   `json:"initialize_db"`
	ShowHelpMessage *bool   `json:"help"`
	ShowVersion     *bool   `json:"version"`
	ConfigPath      *string `json:"config_path"`
}

/* init flags */
func initFlags() {
	appFlags.InitializeDB = flag.Bool("initialize_db", false, "initialize the database and create all tables")
	appFlags.ShowHelpMessage = flag.Bool("help", false, "show help")
	appFlags.ShowVersion = flag.Bool("version", false, "show version")
	appFlags.ConfigPath = flag.String("config", "", "the path to the config file")
	flag.Parse()

}

func boolEnv(key string) (bool, error) {
	val := os.Getenv(key)
	if slices.Contains([]string{"true", "1", "yes", "y"}, val) {
		return true, nil
	}
	if slices.Contains([]string{"false", "0", "no", "n", ""}, val) {
		return false, nil
	}
	return false, fmt.Errorf("%s value must be one of [no, n, false, 0, yes, y, true, 1]", key)
}

func initDB(cfg *clconfig.ClokiConfig) {
	bVal, err := boolEnv("OMIT_CREATE_TABLES")
	if err != nil {
		panic(err)
	}
	if bVal {
		return
	}
	err = ctrl.Init(cfg, "qryn")
	if err != nil {
		panic(err)
	}
	err = ctrl.Rotate(cfg, "qryn")
	if err != nil {
		panic(err)
	}
}

func portCHEnv(cfg *clconfig.ClokiConfig) error {
	if len(cfg.Setting.DATABASE_DATA) > 0 {
		return nil
	}
	cfg.Setting.DATABASE_DATA = []config.ClokiBaseDataBase{{
		ReadTimeout:  30,
		WriteTimeout: 30,
	}}
	db := "cloki"
	if os.Getenv("CLICKHOUSE_DB") != "" {
		db = os.Getenv("CLICKHOUSE_DB")
	}
	cfg.Setting.DATABASE_DATA[0].Name = db
	if os.Getenv("CLUSTER_NAME") != "" {
		cfg.Setting.DATABASE_DATA[0].ClusterName = os.Getenv("CLUSTER_NAME")
		cfg.Setting.DATABASE_DATA[0].Cloud = true
	}
	server := "localhost"
	if os.Getenv("CLICKHOUSE_SERVER") != "" {
		server = os.Getenv("CLICKHOUSE_SERVER")
	}
	cfg.Setting.DATABASE_DATA[0].Host = server
	//TODO: add to readme to change port to tcp (9000) instead of http
	strPort := "9000"
	if os.Getenv("CLICKHOUSE_PORT") != "" {
		strPort = os.Getenv("CLICKHOUSE_PORT")
	}
	port, err := strconv.ParseUint(strPort, 10, 32)
	if err != nil {
		return fmt.Errorf("invalid port number: %w", err)
	}
	cfg.Setting.DATABASE_DATA[0].Port = uint32(port)
	if os.Getenv("CLICKHOUSE_AUTH") != "" {
		auth := strings.SplitN(os.Getenv("CLICKHOUSE_AUTH"), ":", 2)
		cfg.Setting.DATABASE_DATA[0].User = auth[0]
		if len(auth) > 1 {
			cfg.Setting.DATABASE_DATA[0].Password = auth[1]
		}
	}
	if os.Getenv("ADVANCED_SAMPLES_ORDERING") != "" {
		cfg.Setting.DATABASE_DATA[0].SamplesOrdering = os.Getenv("ADVANCED_SAMPLES_ORDERING")
	}
	//TODO: add to readme
	secure := false
	if os.Getenv("CLICKHOUSE_PROTO") == "https" || os.Getenv("CLICKHOUSE_PROTO") == "tls" {
		secure = true
	}
	cfg.Setting.DATABASE_DATA[0].Secure = secure
	if os.Getenv("SELF_SIGNED_CERT") != "" {
		insecureSkipVerify, err := boolEnv("SELF_SIGNED_CERT")
		if err != nil {
			return fmt.Errorf("invalid self_signed_cert value: %w", err)
		}
		cfg.Setting.DATABASE_DATA[0].InsecureSkipVerify = insecureSkipVerify
	}

	cfg.Setting.DATABASE_DATA[0].TTLDays = 7
	if os.Getenv("SAMPLES_DAYS") != "" {
		days, err := strconv.Atoi(os.Getenv("SAMPLES_DAYS"))
		if err != nil {
			return fmt.Errorf("invalid samples_days value: %w", err)
		}
		cfg.Setting.DATABASE_DATA[0].TTLDays = days
	}

	if os.Getenv("STORAGE_POLICY") != "" {
		cfg.Setting.DATABASE_DATA[0].StoragePolicy = os.Getenv("STORAGE_POLICY")
	}

	cfg.Setting.ClokiReader.OmitEmptyValues, err = boolEnv("ADVANCED_OMIT_EMPTY_VALUES")
	if err != nil {
		return fmt.Errorf("invalid ADVANCED_OMIT_EMPTY_VALUES value: %w", err)
	}

	return nil
}

func portEnv(cfg *clconfig.ClokiConfig) error {
	err := portCHEnv(cfg)
	if err != nil {
		return err
	}
	if os.Getenv("QRYN_LOGIN") != "" {
		cfg.Setting.AUTH_SETTINGS.BASIC.Username = os.Getenv("QRYN_LOGIN")
	}
	if os.Getenv("CLOKI_LOGIN") != "" {
		cfg.Setting.AUTH_SETTINGS.BASIC.Username = os.Getenv("CLOKI_LOGIN")
	}
	if os.Getenv("QRYN_PASSWORD") != "" {
		cfg.Setting.AUTH_SETTINGS.BASIC.Password = os.Getenv("QRYN_PASSWORD")
	}
	if os.Getenv("CLOKI_PASSWORD") != "" {
		cfg.Setting.AUTH_SETTINGS.BASIC.Password = os.Getenv("CLOKI_PASSWORD")
	}
	if os.Getenv("CORS_ALLOW_ORIGIN") != "" {
		cfg.Setting.HTTP_SETTINGS.Cors.Enable = true
		cfg.Setting.HTTP_SETTINGS.Cors.Origin = os.Getenv("CORS_ALLOW_ORIGIN")
	}
	if os.Getenv("PORT") != "" {
		port, err := strconv.Atoi(os.Getenv("PORT"))
		if err != nil {
			return fmt.Errorf("invalid port number: %w", err)
		}
		cfg.Setting.HTTP_SETTINGS.Port = port
	}
	if os.Getenv("HOST") != "" {
		cfg.Setting.HTTP_SETTINGS.Host = os.Getenv("HOST")
	}
	if cfg.Setting.HTTP_SETTINGS.Host == "" {
		cfg.Setting.HTTP_SETTINGS.Host = "0.0.0.0"
	}
	if os.Getenv("ADVANCED_PROMETHEUS_MAX_SAMPLES") != "" {
		maxSamples, err := strconv.Atoi(os.Getenv("ADVANCED_PROMETHEUS_MAX_SAMPLES"))
		if err != nil {
			return fmt.Errorf("invalid max samples value `%d`: %w", maxSamples, err)
		}
		cfg.Setting.SYSTEM_SETTINGS.MetricsMaxSamples = maxSamples
	}
	mode := "all"
	if os.Getenv("MODE") != "" {
		mode = os.Getenv("MODE")
	}
	readonly, err := boolEnv("READONLY")
	if err != nil {
		return err
	}
	if readonly && mode == "all" {
		mode = "reader"
	}

	cfg.Setting.SYSTEM_SETTINGS.Mode = mode

	if os.Getenv("BULK_MAX_SIZE_BYTES") != "" {
		maxSize, err := strconv.ParseInt(os.Getenv("BULK_MAX_SIZE_BYTES"), 10, 63)
		if err != nil {
			return fmt.Errorf("invalid max size value `%d`: %w", maxSize, err)
		}
		cfg.Setting.SYSTEM_SETTINGS.DBBulk = maxSize
	}

	strMaxAge := "100"
	if os.Getenv("BULK_MAX_AGE_MS") != "" {
		strMaxAge = os.Getenv("BULK_MAX_AGE_MS")
	}
	maxAge, err := strconv.Atoi(strMaxAge)
	if err != nil {
		return fmt.Errorf("invalid max age value `%d`: %w", maxAge, err)
	}
	cfg.Setting.SYSTEM_SETTINGS.DBTimer = float64(maxAge) / 1000

	// Enable pattern the logs drilldown feature (pattern detection, etc.). Default is "false"
	if os.Getenv("LOG_DRILLDOWN") != "" {
		cfg.Setting.DRILLDOWN_SETTINGS.LogDrilldown, err = boolEnv("LOG_DRILLDOWN")
		if err != nil {
			return err
		}
	}

	// How similar log strings should be to form a pattern (0-1) where:
	//   0 - completely different
	//   1 - same string
	// Default is 0.7
	if os.Getenv("LOG_PATTERN_SIMILARITY") != "" {
		cfg.Setting.DRILLDOWN_SETTINGS.LogPatternsSimilarity, err = strconv.ParseFloat(
			os.Getenv("LOG_PATTERN_SIMILARITY"), 64)
		if err != nil {
			return err
		}
	}

	// How many log patterns should be read upon a read request (default is 300)
	if os.Getenv("LOG_PATTERN_READ_LIMIT") != "" {
		cfg.Setting.DRILLDOWN_SETTINGS.LogPatternsReadLimit, err = strconv.Atoi(
			os.Getenv("LOG_PATTERN_READ_LIMIT"))
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	initFlags()
	var configPaths []string
	if _, err := os.Stat(*appFlags.ConfigPath); err == nil {
		configPaths = append(configPaths, *appFlags.ConfigPath)
	}
	cfg := clconfig.New(clconfig.CLOKI_READER, configPaths, "", "")

	cfg.ReadConfig()

	err := portEnv(cfg)
	if err != nil {
		panic(err)
	}
	if cfg.Setting.HTTP_SETTINGS.Port == 0 {
		cfg.Setting.HTTP_SETTINGS.Port = 3100
	}

	if cfg.Setting.SYSTEM_SETTINGS.Mode == "all" || cfg.Setting.SYSTEM_SETTINGS.Mode == "writer" {
		initDB(cfg)
	}
	if os.Getenv("MODE") == "init_only" {
		return
	}

	app := mux.NewRouter()
	if cfg.Setting.AUTH_SETTINGS.BASIC.Username != "" &&
		cfg.Setting.AUTH_SETTINGS.BASIC.Password != "" {
		app.Use(middleware.BasicAuthMiddleware(cfg.Setting.AUTH_SETTINGS.BASIC.Username,
			cfg.Setting.AUTH_SETTINGS.BASIC.Password))
	}
	app.Use(middleware.AcceptEncodingMiddleware)
	if cfg.Setting.HTTP_SETTINGS.Cors.Enable {
		app.Use(middleware.CorsMiddleware(cfg.Setting.HTTP_SETTINGS.Cors.Origin))
	}
	app.Use(middleware.LoggingMiddleware("[{{.status}}] {{.method}} {{.url}} - LAT:{{.latency}}"))
	commonroutes.RegisterCommonRoutes(app)
	cfg.Setting.LOG_SETTINGS.Level = "debug"
	cfg.Setting.LOG_SETTINGS.Stdout = true
	if cfg.Setting.SYSTEM_SETTINGS.Mode == "all" ||
		cfg.Setting.SYSTEM_SETTINGS.Mode == "writer" ||
		cfg.Setting.SYSTEM_SETTINGS.Mode == "" {
		writer.Init(cfg, app)
	}
	if cfg.Setting.SYSTEM_SETTINGS.Mode == "all" ||
		cfg.Setting.SYSTEM_SETTINGS.Mode == "reader" ||
		cfg.Setting.SYSTEM_SETTINGS.Mode == "" {
		reader.Init(cfg, app)
		view.Init(cfg, app)
	}

	initPyro()

	httpURL := fmt.Sprintf("%s:%d", cfg.Setting.HTTP_SETTINGS.Host, cfg.Setting.HTTP_SETTINGS.Port)
	httpStart(app, httpURL)
}

func httpStart(server *mux.Router, httpURL string) {
	logger.Info("Starting service")
	http.Handle("/", server)
	listener, err := net.Listen("tcp", httpURL)
	if err != nil {
		logger.Error("Error creating listener:", err)
		panic(err)
	}
	logger.Info("Server is listening on", httpURL)
	if err := http.Serve(listener, server); err != nil {
		logger.Error("Error serving:", err)
		panic(err)
	}
}

func initPyro() {
	// Pyroscope configuration
	serverAddress := os.Getenv("PYROSCOPE_SERVER_ADDRESS")
	if serverAddress == "" {
		return
	}

	applicationName := os.Getenv("PYROSCOPE_APPLICATION_NAME")
	if applicationName == "" {
		applicationName = "gigapipe"
	}

	// Initialize Pyroscope
	config := pyroscope.Config{
		ApplicationName: applicationName,
		ServerAddress:   serverAddress,
		Logger:          pyroscope.StandardLogger,
		ProfileTypes: []pyroscope.ProfileType{
			pyroscope.ProfileCPU,
			pyroscope.ProfileAllocObjects,
			pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects,
			pyroscope.ProfileInuseSpace,
		},
	}

	_, err := pyroscope.Start(config)
	if err != nil {
		log.Fatalf("Failed to start Pyroscope: %v", err)
	}
	fmt.Println("Pyroscope profiling started")
}
