package controller

import (
	"context"
	"github.com/metrico/qryn/writer/ch_wrapper"
	config "github.com/metrico/qryn/writer/config"
	"github.com/metrico/qryn/writer/model"
	"github.com/metrico/qryn/writer/pattern/clustering"
	"github.com/metrico/qryn/writer/service"
	"github.com/metrico/qryn/writer/service/registry"
	"github.com/metrico/qryn/writer/utils/logger"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var InsertServiceRegistry registry.IServiceRegistry
var ClusteringService *clustering.LogClusterer
var connFactory ch_wrapper.IChClientFactory

var tokPool = sync.Pool{}
var tokPoolSize int32
var random *rand.Rand
var mtx sync.Mutex

func skipLine() bool {
	if config.Cloki.Setting.DRILLDOWN_SETTINGS.LogPatternsDownsampling == 1 {
		return false
	}
	mtx.Lock()
	defer mtx.Unlock()
	return random.Float64() < (1 - config.Cloki.Setting.DRILLDOWN_SETTINGS.LogPatternsDownsampling)
}

func getToks() []clustering.Token {
	toks := tokPool.Get()
	if toks != nil {
		atomic.AddInt32(&tokPoolSize, -1)
		return toks.([]clustering.Token)
	}
	return make([]clustering.Token, 0, 150)
}
func putToks(toks []clustering.Token) {
	if atomic.LoadInt32(&tokPoolSize) > 100 {
		return
	}
	toks = toks[:0]
	tokPool.Put(toks)
	atomic.AddInt32(&tokPoolSize, 1)
}

func ClusterLines(lines []string, fingerprints []uint64, timestamps []int64) {
	if InsertServiceRegistry == nil {
		return
	}
	go func() {
		toks := getToks()
		defer putToks(toks)
		var logLine clustering.LogLine
		for i, line := range lines {
			if skipLine() {
				continue
			}
			logLine.Line = line
			logLine.Fingerprint = fingerprints[i]
			logLine.TimestampNs = timestamps[i]
			logLine.Tokens = clustering.Lex(line, toks)
			ClusteringService.Add(&logLine)
			toks = toks[:0]
		}
	}()
}

func Init(service registry.IServiceRegistry, conns ch_wrapper.IChClientFactory) {
	InsertServiceRegistry = service
	ClusteringService = clustering.NewLogClusterer()
	connFactory = conns
	err := syncPatterns(time.Minute * 5)
	if err != nil {
		logger.Error("Failed to load patterns: " + err.Error())
	}
	go RunCleanup()
	go RunFlush()
	go RunSync()
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
			MPatternId:       make([]uint64, len(data)),
			MIterationId:     make([]uint64, len(data)),
			//MWriterID:     make([]string, len(data)), TODO
		}

		for i, logLine := range data {
			req.MTimestamp10m[i] = logLine.TimestampS / 600
			req.MFingerprint[i] = logLine.Fingerprint
			req.MTimestampS[i] = logLine.TimestampS
			req.MOverallCost[i] = uint32(logLine.OverallCost)
			req.MGeneralizedCost[i] = uint32(logLine.GeneralizedCost)
			req.MSamplesCount[i] = uint32(logLine.Count)
			req.MPatternId[i] = logLine.PatternId
			req.MIterationId[i] = logLine.IterationId
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

func RunSync() {
	t := time.NewTicker(time.Minute)
	for range t.C {
		err := syncPatterns(time.Second * 90)
		if err != nil {
			logger.Error("Failed to sync patterns: ", err)
		}
	}
}

type PatternsSynchronizer struct {
	since time.Time
}

func (p *PatternsSynchronizer) GetPatternIDs() ([][2]uint64, error) {
	conn, err := connFactory()
	if err != nil {
		return nil, err
	}
	rows, err := conn.Query(context.Background(),
		`SELECT pattern_id, max(iteration_id) 
FROM patterns 
WHERE timestamp_10m >= ? AND timestamp_s >= ? GROUP BY pattern_id`,
		p.since.Unix()/600, p.since.Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var patternID uint64
	var iterationID uint64
	var res [][2]uint64
	for rows.Next() {
		err := rows.Scan(&patternID, &iterationID)
		if err != nil {
			return nil, err
		}
		res = append(res, [2]uint64{patternID, iterationID})
	}
	return res, nil
}

func (p *PatternsSynchronizer) getPatterns(patternIds []uint64,
	conn ch_wrapper.IChClient) ([]clustering.PatternInfo, error) {
	rows, err := conn.Query(context.Background(),
		`SELECT pattern_id, 
       max(iteration_id) as iter, 
       argMax(overall_cost, iteration_id) as over_cost, 
       argMax(generalized_cost, iteration_id) as gen_cost, 
       argMax(tokens, iteration_id) as toks,
       argMax(classes, iteration_id) as clss
FROM patterns 
WHERE timestamp_10m >= ? AND timestamp_s >= ? AND pattern_id IN (?)
GROUP BY pattern_id`,
		p.since.Unix()/600, p.since.Unix(), patternIds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var patternID uint64
	var iterationID uint64
	var overCost, genCost uint32
	var tokens []string
	var classes []uint32
	var res []clustering.PatternInfo
	for rows.Next() {
		err = rows.Scan(&patternID, &iterationID, &overCost, &genCost, &tokens, &classes)
		if err != nil {
			return nil, err
		}
		toks := make([]clustering.Token, len(tokens))
		for i, t := range tokens {
			toks[i] = clustering.Token{
				Value: t,
				Type:  clustering.TokenType(classes[i]),
			}
		}
		res = append(res, clustering.PatternInfo{
			Id:              patternID,
			IterationId:     iterationID,
			OverallCost:     int(overCost),
			GeneralizedCost: int(genCost),
			Tokens:          toks,
		})
	}
	return res, nil
}

func (p *PatternsSynchronizer) GetPatterns(patternIds []uint64) ([]clustering.PatternInfo, error) {
	conn, err := connFactory()
	if err != nil {
		return nil, err
	}
	res := make([]clustering.PatternInfo, 0, len(patternIds))
	for i := len(patternIds); i > 0; i -= 1000 {
		start := max(0, i-1000)
		pats, err := p.getPatterns(patternIds[start:i], conn)
		if err != nil {
			return nil, err
		}
		res = append(res, pats...)
	}
	return res, nil
}

func syncPatterns(d time.Duration) error {
	since := time.Now().Add(-d)
	ps := &PatternsSynchronizer{since: since}
	return ClusteringService.SyncPatterns(ps)
}
