package clustering

import (
	"strings"
	"sync"
	"time"
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

const (
	DefaultSimilarity = 0.8 // Similarity threshold for clustering.
)

type LogClusterSample struct {
	Tokens          []Token
	Fingerprint     uint64
	TimestampS      uint32
	Count           int
	OverallCost     int
	GeneralizedCost int
}

// LogCluster represents a log message cluster.
type LogCluster struct {
	Tokens    []Token
	LineCount int64
	Samples   []LogClusterSample

	OverallCost     int
	GeneralizedCost int
	ID              LogClusterId
	lastGeneralized time.Time
}

func (c *LogCluster) generalize(line *LogLine) bool {
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
	c.lastGeneralized = time.Now()

	lastIdx := len(c.Samples) - 1
	timestampS := uint32(line.TimestampNs / 1000000000)
	if lastIdx != 0 || c.Samples[lastIdx].TimestampS != timestampS {
		c.Samples = append(
			c.Samples,
			LogClusterSample{
				Fingerprint: line.Fingerprint,
				TimestampS:  timestampS,
				Count:       1,
			})
	} else {
		c.Samples[len(c.Samples)-1].Count++
	}
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
	if overallSamples < 2 {
		return nil
	}
	samples := c.Samples
	tokens := append([]Token{}, c.Tokens...)
	for i := range c.Samples {
		samples[i].Tokens = tokens
		samples[i].OverallCost = c.OverallCost
		samples[i].GeneralizedCost = c.GeneralizedCost
	}
	c.Samples = nil
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
	m        sync.Mutex
}

func (r *LogClusterRow) add(line *LogLine) {
	r.m.Lock()
	defer r.m.Unlock()
	for _, clust := range r.clusters {
		if clust.generalize(line) {
			return
		}
	}
	r.clusters = append(r.clusters,
		&LogCluster{
			Tokens:    line.Tokens,
			LineCount: 1,
			Samples: []LogClusterSample{{
				Fingerprint: line.Fingerprint,
				TimestampS:  uint32(line.TimestampNs / 1000000000),
				Count:       1,
			}},
			OverallCost:     getOverallCost(line.Tokens),
			GeneralizedCost: 0,
			ID:              LogClusterId{},
			lastGeneralized: time.Now(),
		})
}

func (r *LogClusterRow) flush() []LogClusterSample {
	r.m.Lock()
	defer r.m.Unlock()
	var samples []LogClusterSample
	for _, clust := range r.clusters {
		samples = append(samples, clust.flush()...)
	}
	r.clusters = nil
	return samples
}

func (r *LogClusterRow) cleanup() {
	r.m.Lock()
	defer r.m.Unlock()
	for i := len(r.clusters) - 1; i >= 0; i-- {
		if time.Since(r.clusters[i].lastGeneralized) > time.Minute*5 {
			copy(r.clusters[i:], r.clusters[i+1:])
			r.clusters = r.clusters[:len(r.clusters)-1]
		}
	}
}

type LogClusterer struct {
	clusters map[LogClusterId]*LogClusterRow
	m        sync.RWMutex
}

func NewLogClusterer() *LogClusterer {
	return &LogClusterer{
		clusters: make(map[LogClusterId]*LogClusterRow),
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
	c.m.Lock()
	defer c.m.Unlock()
	clusters, ok := c.clusters[id]
	if !ok {
		c.clusters[id] = &LogClusterRow{}
		return c.clusters[id]
	}
	return clusters
}

func (c *LogClusterer) Add(log *LogLine) {
	id := getLogId(log)
	row := c.get(id)
	c.m.RLock()
	defer c.m.RUnlock()
	row.add(log)
}

func (c *LogClusterer) Flush() []LogClusterSample {
	c.m.Lock()
	defer c.m.Unlock()
	var samples []LogClusterSample
	for _, row := range c.clusters {
		samples = append(samples, row.flush()...)
	}
	return samples
}

func (c *LogClusterer) Cleanup() {
	c.m.Lock()
	defer c.m.Unlock()
	var toDelete []LogClusterId
	// Waiting for all the pending Add to finish
	time.Sleep(time.Millisecond)
	for k, row := range c.clusters {
		row.cleanup()
		toDelete = append(toDelete, k)
	}
	for _, id := range toDelete {
		delete(c.clusters, id)
	}
}
