package logger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"log/syslog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/metrico/qryn/reader/config"
	"github.com/metrico/qryn/reader/system"
	"github.com/sirupsen/logrus"
)

type LogInfo logrus.Fields

var RLogs *rotatelogs.RotateLogs
var Logger = logrus.New()

type DbLogger struct{}

/* db logger for logrus */
func (*DbLogger) Print(v ...interface{}) {
	if v[0] == "sql" {
		Logger.WithFields(logrus.Fields{"module": "db", "type": "sql"}).Print(v[3])
	}
	if v[0] == "log" {
		Logger.WithFields(logrus.Fields{"module": "db", "type": "log"}).Print(v[2])
	}
}

// initLogger function
func InitLogger() {

	//env := os.Getenv("environment")
	//isLocalHost := env == "local"
	if config.Cloki.Setting.LOG_SETTINGS.Json {
		// Log as JSON instead of the default ASCII formatter.
		Logger.SetFormatter(&logrus.JSONFormatter{})
	} else {
		Logger.Formatter.(*logrus.TextFormatter).DisableTimestamp = false
		Logger.Formatter.(*logrus.TextFormatter).DisableColors = true
	}

	if config.Cloki.Setting.LOG_SETTINGS.Qryn.Url != "" {
		hostname := ""
		if config.Cloki.Setting.LOG_SETTINGS.Qryn.AddHostname {
			hostname, _ = os.Hostname()
		}

		headers := map[string]string{}
		for _, h := range strings.Split(config.Cloki.Setting.LOG_SETTINGS.Qryn.Headers, ";;") {
			pair := strings.Split(h, ":")
			headers[pair[0]] = strings.Join(pair[1:], ":")
		}

		qrynFmt := &qrynFormatter{
			formatter: Logger.Formatter,
			url:       config.Cloki.Setting.LOG_SETTINGS.Qryn.Url,
			app:       config.Cloki.Setting.LOG_SETTINGS.Qryn.App,
			hostname:  hostname,
			headers:   headers,
		}
		Logger.SetFormatter(qrynFmt)
		qrynFmt.Run()
	}

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	if config.Cloki.Setting.LOG_SETTINGS.Stdout {
		Logger.SetOutput(os.Stdout)
		log.SetOutput(os.Stdout)
	}

	/* log level default */
	if config.Cloki.Setting.LOG_SETTINGS.Level == "" {
		config.Cloki.Setting.LOG_SETTINGS.Level = "error"
	}

	if logLevel, ok := logrus.ParseLevel(config.Cloki.Setting.LOG_SETTINGS.Level); ok == nil {
		// Only log the warning severity or above.
		Logger.SetLevel(logLevel)
	} else {
		Logger.Error("Couldn't parse loglevel", config.Cloki.Setting.LOG_SETTINGS.Level)
		Logger.SetLevel(logrus.ErrorLevel)
	}

	Logger.Info("init logging system")

	if !config.Cloki.Setting.LOG_SETTINGS.Stdout && !config.Cloki.Setting.LOG_SETTINGS.SysLog {
		// configure file system hook
		configureLocalFileSystemHook()
	} else if !config.Cloki.Setting.LOG_SETTINGS.Stdout {
		configureSyslogHook()
	}
}

// SetLoggerLevel function
func SetLoggerLevel(loglevelString string) {

	if logLevel, ok := logrus.ParseLevel(loglevelString); ok == nil {
		// Only log the warning severity or above.
		Logger.SetLevel(logLevel)
	} else {
		Logger.Error("Couldn't parse loglevel", loglevelString)
		Logger.SetLevel(logrus.ErrorLevel)
	}
}

func configureLocalFileSystemHook() {

	logPath := config.Cloki.Setting.LOG_SETTINGS.Path
	logName := config.Cloki.Setting.LOG_SETTINGS.Name
	var err error

	if configPath := os.Getenv("WEBAPPLOGPATH"); configPath != "" {
		logPath = configPath
	}

	if configName := os.Getenv("WEBAPPLOGNAME"); configName != "" {
		logName = configName
	}

	fileLogExtension := filepath.Ext(logName)
	fileLogBase := strings.TrimSuffix(logName, fileLogExtension)

	pathAllLog := logPath + "/" + fileLogBase + "_%Y%m%d%H%M" + fileLogExtension
	pathLog := logPath + "/" + logName

	RLogs, err = rotatelogs.New(
		pathAllLog,
		rotatelogs.WithLinkName(pathLog),
		rotatelogs.WithMaxAge(time.Duration(config.Cloki.Setting.LOG_SETTINGS.MaxAgeDays)*time.Hour),
		rotatelogs.WithRotationTime(time.Duration(config.Cloki.Setting.LOG_SETTINGS.RotationHours)*time.Hour),
	)

	if err != nil {
		Logger.Println("Local file system hook initialize fail")
		return
	}

	Logger.SetOutput(RLogs)
	log.SetOutput(RLogs)

	/*
		Logger.AddHook(lfshook.NewHook(lfshook.WriterMap{
			logrus.InfoLevel:  rLogs,
			logrus.DebugLevel: rLogs,
			logrus.ErrorLevel: rLogs,
		}, &logrus.JSONFormatter{}))
	*/
}
func configureSyslogHook() {

	var err error

	Logger.Println("Init syslog...")

	sevceritySyslog := getSevirtyByName(config.Cloki.Setting.LOG_SETTINGS.SysLogLevel)

	syslogger, err := syslog.New(sevceritySyslog, "hepic-app-server")

	//hook, err := lSyslog.NewSyslogHook(proto, logSyslogUri, sevceritySyslog, "")

	if err != nil {
		Logger.Println("Unable to connect to syslog:", err)
	}

	Logger.SetOutput(syslogger)
	log.SetOutput(syslogger)

	/*
		Logger.AddHook(lfshook.NewHook(lfshook.WriterMap{
			logrus.InfoLevel:  rLogs,
			logrus.DebugLevel: rLogs,
			logrus.ErrorLevel: rLogs,
		}, &logrus.JSONFormatter{}))
	*/
}

func Info(args ...interface{}) {
	Logger.Info(args...)
}

func Error(args ...interface{}) {
	Logger.Error(args...)
}

func Debug(args ...interface{}) {
	Logger.Debug(args...)
}

func getSevirtyByName(sevirity string) syslog.Priority {

	switch sevirity {
	case system.SYSLOG_LOG_EMERG:
		return syslog.LOG_EMERG
	case system.SYSLOG_LOG_ALERT:
		return syslog.LOG_ALERT
	case system.SYSLOG_LOG_CRIT:
		return syslog.LOG_CRIT
	case system.SYSLOG_LOG_ERR:
		return syslog.LOG_ERR
	case system.SYSLOG_LOG_WARNING:
		return syslog.LOG_WARNING
	case system.SYSLOG_LOG_NOTICE:
		return syslog.LOG_NOTICE
	case system.SYSLOG_LOG_INFO:
		return syslog.LOG_INFO
	case system.SYSLOG_LOG_DEBUG:
		return syslog.LOG_DEBUG
	default:
		return syslog.LOG_INFO

	}
}

type qrynFormatter struct {
	mtx          sync.Mutex
	formatter    logrus.Formatter
	bufferToQryn []*logrus.Entry
	timer        *time.Ticker
	url          string
	app          string
	hostname     string
	headers      map[string]string
}

type qrynLogs struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

func (q *qrynFormatter) Format(e *logrus.Entry) ([]byte, error) {
	q.mtx.Lock()
	q.bufferToQryn = append(q.bufferToQryn, e)
	q.mtx.Unlock()
	return q.formatter.Format(e)
}

func (q *qrynFormatter) Run() {
	q.timer = time.NewTicker(time.Second)
	go func() {
		for range q.timer.C {
			q.mtx.Lock()
			bufferToQryn := q.bufferToQryn
			q.bufferToQryn = nil
			q.mtx.Unlock()
			if len(bufferToQryn) < 1 {
				continue
			}

			streams := map[string]*qrynLogs{}
			for _, e := range bufferToQryn {
				stream := map[string]string{}
				stream["app"] = q.app
				if q.hostname != "" {
					stream["hostname"] = q.hostname
				}
				stream["level"] = e.Level.String()

				strStream := fmt.Sprintf("%v", stream)
				if _, ok := streams[strStream]; !ok {
					streams[strStream] = &qrynLogs{Stream: stream}
				}

				strValue, _ := q.formatter.Format(e)
				streams[strStream].Values = append(
					streams[strStream].Values,
					[]string{strconv.FormatInt(e.Time.UnixNano(), 10), string(strValue)})
			}

			var arrStreams []*qrynLogs
			for _, s := range streams {
				arrStreams = append(arrStreams, s)
			}

			strStreams, _ := json.Marshal(map[string][]*qrynLogs{"streams": arrStreams})
			go func() {
				req, _ := http.NewRequest("POST", q.url, bytes.NewReader(strStreams))
				if req == nil {
					return
				}
				for k, v := range q.headers {
					req.Header.Set(k, v)
				}
				req.Header.Set("Content-Type", "application/json")
				http.DefaultClient.Do(req)
			}()
		}
	}()
}
