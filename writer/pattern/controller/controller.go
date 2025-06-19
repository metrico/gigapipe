package controller

import (
	"github.com/metrico/qryn/writer/model"
	"github.com/metrico/qryn/writer/pattern/clustering"
	"github.com/metrico/qryn/writer/service"
	"github.com/metrico/qryn/writer/service/registry"
	"github.com/metrico/qryn/writer/utils/logger"
	"time"
)

var InsertServiceRegistry registry.IServiceRegistry
var ClusteringService *clustering.LogClusterer

func ClusterLines(lines []string, fingerprints []uint64, timestamps []int64) {
	if InsertServiceRegistry == nil {
		return
	}
	go func() {
		var logLine clustering.LogLine
		for i, line := range lines {
			logLine.Line = line
			logLine.Fingerprint = fingerprints[i]
			logLine.TimestampNs = timestamps[i]
			logLine.Tokens = clustering.Lex(line)
			ClusteringService.Add(&logLine)
		}
	}()
}

func Init(service registry.IServiceRegistry) {
	InsertServiceRegistry = service
	ClusteringService = clustering.NewLogClusterer()
	go RunCleanup()
	go RunFlush()
}

func RunCleanup() {
	t := time.NewTicker(time.Second * 30)
	for range t.C {
		ClusteringService.Cleanup()
	}
}

func RunFlush() {
	t := time.NewTicker(time.Second)
	for range t.C {
		data := ClusteringService.Flush()
		req := model.PatternsData{
			MTimestamp10m:    make([]uint32, len(data)),
			MFingerprint:     make([]uint64, len(data)),
			MTimestampS:      make([]uint32, len(data)),
			MTokens:          make([][]string, len(data)),
			MClasses:         make([][]uint32, len(data)),
			MOverallCost:     make([]uint32, len(data)),
			MGeneralizedCost: make([]uint32, len(data)),
			MSamplesCount:    make([]uint32, len(data)),
			//MWriterID:     make([]string, len(data)), TODO
		}

		for i, logLine := range data {
			req.MTimestamp10m[i] = logLine.TimestampS / 600
			req.MFingerprint[i] = logLine.Fingerprint
			req.MTimestampS[i] = logLine.TimestampS
			req.MOverallCost[i] = uint32(logLine.OverallCost)
			req.MGeneralizedCost[i] = uint32(logLine.GeneralizedCost)
			req.MSamplesCount[i] = uint32(logLine.Count)
			for _, t := range logLine.Tokens {
				req.MTokens[i] = append(req.MTokens[i], t.Value)
				req.MClasses[i] = append(req.MClasses[i], uint32(t.Type))
			}
		}
		svc, err := InsertServiceRegistry.GetPatternInsertService("")
		if err != nil {
			logger.Error("Failed to get pattern insert service: ", err)
			continue
		}
		go func() {
			_, err := svc.Request(&req, service.INSERT_MODE_DEFAULT).Get()
			if err != nil {
				logger.Error("Failed to flush clustered lines: ", err)
			}
		}()
	}
}
