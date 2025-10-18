package clustering

import (
	"strings"
	"sync"
	"time"

	"github.com/go-faster/city"
	"github.com/metrico/qryn/v4/writer/config"
	"github.com/metrico/qryn/v4/writer/utils/logger"
)

type LogLine struct {
	Line        string
	Tokens      []Token
	Fingerprint uint64
	TimestampNs int64
}

// Token represents a single token with its type.
type Token struct {
	Value string
	Type  TokenType
}

const (
	Generalized TokenType = iota
	UUID        TokenType = iota
	IPAddress
	Timestamp
	Number
	PID
	Special
	Priority

	// Valuable Tokens
	HTTPMethod
	HTTPCode
	ProgramName
	Word
	LogLevel
	HTTPPathPart
	HTTPVersion
)

var tokenCosts = []int{
	0,  // Generalized
	1,  // UUID
	2,  // IPAddress
	3,  // Timestamp
	4,  // Number
	5,  // PID
	6,  // Special
	7,  // Priority
	30, // HTTPMethod
	31, // HTTPCode
	32, // ProgramName
	33, // Word
	34, // LogLevel
	35, // HTTPPathPart
	36, // HTTPVersion
}

const Placeholder = "<_>"

// TokenType identifies the type of token.
type TokenType int

// ------------------------ Drain Clustering Section ------------------------

type LogClusterSample struct {
	Tokens          []Token
	Fingerprint     uint64
	TimestampS      uint32
	Count           int
	OverallCost     int
	GeneralizedCost int
	PatternId       uint64
	IterationId     uint64
}

// LogCluster represents a log message cluster.
type LogCluster struct {
	PatternId   uint64
	IterationId uint64
	Tokens      []Token
	LineCount   int64
	Samples     []LogClusterSample

	OverallCost     int
	GeneralizedCost int
	ID              LogClusterId
	lastFlush       time.Time
	matcher         patternMatcherV1
}

func newLogCluster(line *LogLine) *LogCluster {
	tokens := make([]Token, len(line.Tokens))
	copy(tokens, line.Tokens)
	res := &LogCluster{
		Tokens:      tokens,
		PatternId:   city.CH64([]byte(line.Line)),
		IterationId: uint64(time.Now().UnixNano()),
		LineCount:   1,
		Samples: []LogClusterSample{{
			Fingerprint: line.Fingerprint,
			TimestampS:  uint32(line.TimestampNs / 1000000000),
			Count:       1,
		}},
		OverallCost:     getOverallCost(line.Tokens),
		GeneralizedCost: 0,
		ID:              LogClusterId{},
		lastFlush:       time.Now(),
		matcher:         newPatternMatcherV1(line.Tokens),
	}
	return res
}

func loadLogCluster(info *PatternInfo) *LogCluster {
	tokens := make([]Token, len(info.Tokens))
	copy(tokens, info.Tokens)
	res := &LogCluster{
		Tokens:          tokens,
		PatternId:       info.Id,
		IterationId:     info.IterationId,
		OverallCost:     info.OverallCost,
		GeneralizedCost: info.GeneralizedCost,
		ID:              LogClusterId{},
		lastFlush:       time.Now(),
		matcher:         newPatternMatcherV1(info.Tokens),
	}
	return res
}

func (c *LogCluster) addSample(line *LogLine) {
	l := len(c.Samples)
	timestampS := uint32(line.TimestampNs / 1000000000)
	if l != 0 && c.Samples[l-1].Fingerprint == line.Fingerprint && c.Samples[l-1].TimestampS == timestampS {
		c.Samples[l-1].Count++
		return
	}
	c.Samples = append(c.Samples, LogClusterSample{
		Fingerprint: line.Fingerprint,
		TimestampS:  timestampS,
		Count:       1,
	})
}

func (c *LogCluster) sync(iteration uint64, tokens []Token) {
	c.Tokens = tokens
	c.IterationId = iteration
	c.matcher = newPatternMatcherV1(tokens)
}

func (c *LogCluster) generalize(line *LogLine) bool {
	if c.matcher.match(line.Line) {
		c.addSample(line)
		return true
	}
	difference := make([]byte, (len(line.Tokens)+7)/8)
	differenceCost := 0
	i := 0
	j := 0
	for i < len(c.Tokens) && j < len(line.Tokens) {
		if c.Tokens[i].Value == line.Tokens[j].Value && c.Tokens[i].Type != Generalized {
			i++
			j++
			continue
		}
		if c.Tokens[i].Type == Generalized {
			i++
			j++
			continue
		}
		difference[i/8] |= 1 << (i % 8)
		maxCost := max(tokenCosts[line.Tokens[j].Type], tokenCosts[c.Tokens[i].Type])
		differenceCost += maxCost
		i++
		j++
	}
	var DefaultSimilarity = config.Cloki.Setting.DRILLDOWN_SETTINGS.LogPatternsSimilarity
	if (float64(differenceCost+c.GeneralizedCost) / float64(c.OverallCost)) > (1 - DefaultSimilarity) {
		return false
	}
	for i := range c.Tokens {
		if difference[i/8]&(1<<(i%8)) != 0 {
			c.Tokens[i].Type = Generalized
			c.Tokens[i].Value = Placeholder
		}
	}
	c.GeneralizedCost += differenceCost
	if differenceCost != 0 {
		c.IterationId = uint64(time.Now().UnixNano())
		c.matcher = newPatternMatcherV1(c.Tokens)
	}
	c.addSample(line)
	return true
}

func (c *LogCluster) String() string {
	res := strings.Builder{}
	for i, t := range c.Tokens {
		if i > 0 && t.Type == Generalized && c.Tokens[i-1].Type == Generalized {
			continue
		}
		res.WriteString(t.Value)
	}
	return res.String()
}

func (c *LogCluster) flush() []LogClusterSample {
	overallSamples := 0
	for _, s := range c.Samples {
		overallSamples += s.Count
	}
	if overallSamples < 10 {
		return nil
	}
	samples := c.Samples
	tokens := append([]Token{}, c.Tokens...)
	for i := range c.Samples {
		samples[i].PatternId = c.PatternId
		samples[i].IterationId = c.IterationId
		samples[i].Tokens = tokens
		samples[i].OverallCost = c.OverallCost
		samples[i].GeneralizedCost = c.GeneralizedCost
	}
	c.Samples = nil
	c.lastFlush = time.Now()
	return samples
}

type LogClusterId struct {
	TokensCount int
	FirstToken  Token
}

func getFirstValuableTokenIdx(tokens []Token) int {
	if len(tokens) == 0 {
		return -1
	}
	maxTokenId := 0
	maxTokenValue := tokenCosts[tokens[0].Type]
	for i, t := range tokens {
		if tokenCosts[t.Type] >= tokenCosts[HTTPMethod] {
			return i
		}
		if tokenCosts[t.Type] > maxTokenValue {
			maxTokenId = i
			maxTokenValue = tokenCosts[t.Type]
		}
	}
	return maxTokenId
}

func getLogId(log *LogLine) LogClusterId {
	if len(log.Tokens) == 0 {
		return LogClusterId{}
	}
	valuableTokenIdx := getFirstValuableTokenIdx(log.Tokens)
	return LogClusterId{
		TokensCount: len(log.Tokens),
		FirstToken:  log.Tokens[valuableTokenIdx],
	}
}

type LogClusterRow struct {
	clusters []*LogCluster
	m        sync.RWMutex
}

func (r *LogClusterRow) add(line *LogLine) {
	r.m.Lock()
	defer r.m.Unlock()
	for _, clust := range r.clusters {
		if clust.generalize(line) {
			return
		}
	}
	r.clusters = append(r.clusters, newLogCluster(line))
}

func (r *LogClusterRow) flush() []LogClusterSample {
	r.m.Lock()
	defer r.m.Unlock()
	var samples []LogClusterSample
	for _, clust := range r.clusters {
		samples = append(samples, clust.flush()...)
	}
	return samples
}

func (r *LogClusterRow) match(l *LogLine) bool {
	r.m.RLock()
	defer r.m.RUnlock()
	var res bool
	for _, c := range r.clusters {
		if c.matcher.match(l.Line) {
			res = true
			c.addSample(l)
		}
	}
	return res
}

func (r *LogClusterRow) cleanup() {
	r.m.Lock()
	defer r.m.Unlock()
	for i := len(r.clusters) - 1; i >= 0; i-- {
		if time.Since(r.clusters[i].lastFlush) > time.Minute*5 {
			copy(r.clusters[i:], r.clusters[i+1:])
			r.clusters = r.clusters[:len(r.clusters)-1]
		}
	}
}

type LogClusterer struct {
	clusters map[string]map[LogClusterId]*LogClusterRow
	m        sync.RWMutex
}

func NewLogClusterer() *LogClusterer {
	return &LogClusterer{
		clusters: make(map[string]map[LogClusterId]*LogClusterRow),
	}
}

func getOverallCost(tokens []Token) int {
	res := 0
	for i := range tokens {
		res += tokenCosts[tokens[i].Type]
	}
	return res
}

func (c *LogClusterer) get(id LogClusterId) *LogClusterRow {
	c.m.RLock()
	defer c.m.RUnlock()
	if _, ok := c.clusters[id.FirstToken.Value]; !ok {
		return nil
	}
	return c.clusters[id.FirstToken.Value][id]
}

func (c *LogClusterer) getOrCreate(id LogClusterId) *LogClusterRow {
	c.m.Lock()
	defer c.m.Unlock()
	if _, ok := c.clusters[id.FirstToken.Value]; !ok {
		c.clusters[id.FirstToken.Value] = make(map[LogClusterId]*LogClusterRow)
	}
	if _, ok := c.clusters[id.FirstToken.Value][id]; !ok {
		c.clusters[id.FirstToken.Value][id] = &LogClusterRow{}
	}
	return c.clusters[id.FirstToken.Value][id]
}

func (c *LogClusterer) checkMatch(id LogClusterId, log *LogLine) bool {
	c.m.RLock()
	defer c.m.RUnlock()
	var matched bool
	for _, _c := range c.clusters[id.FirstToken.Value] {
		matched = matched || _c.match(log)
	}
	return matched
}

func (c *LogClusterer) Add(log *LogLine) {
	if len(log.Tokens) == 0 || log.Line == "" {
		return
	}
	id := getLogId(log)
	row := c.get(id)
	if row == nil {
		row = c.getOrCreate(id)
	}
	if !c.checkMatch(id, log) {
		row.add(log)
	}
}

func (c *LogClusterer) Flush() []LogClusterSample {
	c.m.Lock()
	defer c.m.Unlock()
	var samples []LogClusterSample
	for _, _row := range c.clusters {
		for _, row := range _row {
			samples = append(samples, row.flush()...)
		}
	}
	return samples
}

func (c *LogClusterer) Cleanup() {
	c.m.Lock()
	defer c.m.Unlock()
	var toDelete []LogClusterId
	for _, _row := range c.clusters {
		for k, row := range _row {
			row.cleanup()
			if len(row.clusters) == 0 {
				toDelete = append(toDelete, k)
			}
		}
	}
	var _toDelete []string
	for _, id := range toDelete {
		delete(c.clusters[id.FirstToken.Value], id)
		if len(c.clusters[id.FirstToken.Value]) == 0 {
			_toDelete = append(_toDelete, id.FirstToken.Value)
		}
	}
	for _, id := range _toDelete {
		delete(c.clusters, id)
	}
}

type PatternSyncSvc interface {
	GetPatternIDs() ([][2]uint64, error)
	GetPatterns(patternIds []uint64) ([]PatternInfo, error)
}

type PatternInfo struct {
	Id              uint64
	IterationId     uint64
	OverallCost     int
	GeneralizedCost int
	Tokens          []Token
}

func (c *LogClusterer) SyncPatterns(svc PatternSyncSvc) error {
	c.m.Lock()
	defer c.m.Unlock()
	logger.Info("Syncing clusters...")
	start := time.Now()
	var patternsToRequest []uint64
	patternIds, err := svc.GetPatternIDs()
	if err != nil {
		return err
	}
	patternIDsMap := map[uint64]uint64{}
	for _, id := range patternIds {
		if iter, ok := patternIDsMap[id[0]]; !ok || iter < id[1] {
			patternIDsMap[id[0]] = id[1]
		}
	}
	for _, __clust := range c.clusters {
		for _, _clust := range __clust {
			for _, clust := range _clust.clusters {
				iterationId := patternIDsMap[clust.PatternId]
				if iterationId == 0 {
					continue
				}
				if iterationId <= clust.IterationId {
					delete(patternIDsMap, clust.PatternId)
					continue
				}
				patternsToRequest = append(patternsToRequest, clust.PatternId)
			}
		}
	}
	for patternId := range patternIDsMap {
		patternsToRequest = append(patternsToRequest, patternId)
	}

	patterns, err := svc.GetPatterns(patternsToRequest)
	if err != nil {
		return err
	}
	patternsMap := make(map[uint64]*PatternInfo)
	for _, pattern := range patterns {
		patternsMap[pattern.Id] = &pattern
	}
	var clusterSynced int
	var clusterAdded int
	for _, __clust := range c.clusters {
		for _, _clust := range __clust {
			for _, clust := range _clust.clusters {
				newPattern := patternsMap[clust.PatternId]
				if newPattern != nil {
					clust.sync(newPattern.IterationId, newPattern.Tokens)
					delete(patternsMap, clust.PatternId)
					clusterSynced++
				}
			}
		}
	}
	clusterAdded = len(patternsMap)
	for _, p := range patternsMap {
		patId := getLogId(&LogLine{Tokens: p.Tokens})
		if _, ok := c.clusters[patId.FirstToken.Value]; !ok {
			c.clusters[patId.FirstToken.Value] = make(map[LogClusterId]*LogClusterRow)
		}
		if _, ok := c.clusters[patId.FirstToken.Value][patId]; !ok {
			c.clusters[patId.FirstToken.Value][patId] = &LogClusterRow{}
		}
		c.clusters[patId.FirstToken.Value][patId].clusters = append(c.clusters[patId.FirstToken.Value][patId].clusters,
			loadLogCluster(p))
	}
	logger.Info("Cluster sync completed in ", time.Since(start), ": ", clusterSynced, " clusters synced, ", clusterAdded, " clusters added")
	return nil
}
