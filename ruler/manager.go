package ruler

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/metrico/qryn/v5/writer/utils/logger"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
)

// PrometheusRule is one recording rule in the Prometheus /api/v1/rules format.
type PrometheusRule struct {
	Name           string            `json:"name"`
	Query          string            `json:"query"`
	Labels         map[string]string `json:"labels,omitempty"`
	Health         string            `json:"health"`
	LastError      string            `json:"lastError"`
	Type           string            `json:"type"`
	LastEvaluation string            `json:"lastEvaluation"`
	EvaluationTime float64           `json:"evaluationTime"`
}

// PrometheusGroup is a rule group in the Prometheus /api/v1/rules format.
type PrometheusGroup struct {
	Name           string           `json:"name"`
	File           string           `json:"file"`
	Rules          []PrometheusRule `json:"rules"`
	Interval       float64          `json:"interval"`
	Limit          int              `json:"limit"`
	LastEvaluation string           `json:"lastEvaluation"`
	EvaluationTime float64          `json:"evaluationTime"`
}

// RuleHealth is the last evaluation outcome for a single rule.
type RuleHealth struct {
	Health         string // "ok" or "err"
	LastError      string
	LastEvalTime   time.Time
	EvaluationTime float64 // seconds
}

// intervalRoutine evaluates all rules sharing one interval at each point of
// the interval's grid.
type intervalRoutine struct {
	interval time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
}

type clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// gridFloor returns now floored to a multiple of interval since the Unix
// epoch, in UTC.
func gridFloor(now time.Time, interval time.Duration) time.Time {
	ms := interval.Milliseconds()
	return time.UnixMilli(now.UnixMilli() / ms * ms).UTC()
}

// RuleManager evaluates recording rules on a schedule and writes results back.
// It re-reads rule groups from storage each cycle, so changes take effect
// without restart. Single-tenant and recording-only: alerting rules are never
// evaluated.
type RuleManager struct {
	evaluator RuleEvaluator
	reader    RuleReader
	writer    RecordingRuleWriter

	// health keyed by namespace:group:record; always in memory.
	health sync.Map

	// lastSeries holds each rule's last written label sets, keyed by
	// ruleSeriesKey and then by label-set hash.
	lastSeries    map[string]map[uint64]labels.Labels
	lastSeriesMtx sync.Mutex

	routines    map[time.Duration]*intervalRoutine
	routinesMtx sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	pollInterval time.Duration
	clock        clock
}

// NewRuleManager builds a manager from its dependencies.
func NewRuleManager(evaluator RuleEvaluator, reader RuleReader, writer RecordingRuleWriter, pollInterval time.Duration) *RuleManager {
	return &RuleManager{
		evaluator:    evaluator,
		reader:       reader,
		writer:       writer,
		routines:     make(map[time.Duration]*intervalRoutine),
		lastSeries:   make(map[string]map[uint64]labels.Labels),
		pollInterval: pollInterval,
		clock:        realClock{},
	}
}

// Start seeds interval routines from current rules and polls for changes.
func (m *RuleManager) Start(ctx context.Context) error {
	m.ctx, m.cancel = context.WithCancel(ctx)
	logger.Info("RuleManager: starting, poll interval ", m.pollInterval.String())

	groups, err := m.reader.GetAllRuleGroups(m.ctx)
	if err != nil {
		return fmt.Errorf("ruler: load initial rules: %w", err)
	}
	m.updateRoutines(groups)

	m.wg.Add(1)
	go m.pollForChanges()
	return nil
}

// Stop cancels all routines and waits for in-flight evaluations to finish.
func (m *RuleManager) Stop() error {
	if m.cancel == nil {
		return nil
	}
	m.cancel()

	m.routinesMtx.Lock()
	for _, routine := range m.routines {
		routine.cancel()
	}
	m.routinesMtx.Unlock()

	m.wg.Wait()
	logger.Info("RuleManager: stopped")
	return nil
}

// updateRoutines starts a routine per distinct valid interval and stops
// routines whose interval is no longer used. Invalid intervals are skipped.
func (m *RuleManager) updateRoutines(groups NamespaceRuleGroups) {
	intervals := make(map[time.Duration]bool)
	for _, gs := range groups {
		for _, g := range gs {
			d, err := time.ParseDuration(g.Interval)
			if err == nil && (d < time.Millisecond || d%time.Millisecond != 0) {
				err = fmt.Errorf("interval %s is not a positive whole number of milliseconds", g.Interval)
			}
			if err != nil {
				logger.Error("RuleManager: skipping group with invalid interval ", g.Name, ": ", err.Error())
				continue
			}
			intervals[d] = true
		}
	}

	m.routinesMtx.Lock()
	for interval, routine := range m.routines {
		if !intervals[interval] {
			routine.cancel()
			delete(m.routines, interval)
		}
	}
	for interval := range intervals {
		if _, exists := m.routines[interval]; exists {
			continue
		}
		ctx, cancel := context.WithCancel(m.ctx)
		routine := &intervalRoutine{
			interval: interval,
			ctx:      ctx,
			cancel:   cancel,
		}
		m.routines[interval] = routine
		m.wg.Add(1)
		go m.runIntervalRoutine(routine)
	}
	m.routinesMtx.Unlock()

	// Reconcile health with the live rule set so entries for rules that have
	// been deleted or renamed do not accumulate in the map forever.
	m.pruneHealth(groups)
	m.pruneLastSeries(groups)
}

// pruneHealth drops health entries whose rule no longer exists in groups,
// bounding the health map to the current set of recording rules.
func (m *RuleManager) pruneHealth(groups NamespaceRuleGroups) {
	valid := make(map[string]struct{})
	for namespace, gs := range groups {
		for _, g := range gs {
			for _, rule := range g.Rules {
				if rule.IsRecording() {
					valid[ruleHealthKey(namespace, g.Name, rule.Record)] = struct{}{}
				}
			}
		}
	}
	m.health.Range(func(k, _ any) bool {
		if _, ok := valid[k.(string)]; !ok {
			m.health.Delete(k)
		}
		return true
	})
}

// runIntervalRoutine evaluates at each grid point of the interval, starting
// with the first one after now. A wake-up past a later grid point evaluates
// at that point and skips the ones in between.
func (m *RuleManager) runIntervalRoutine(routine *intervalRoutine) {
	defer m.wg.Done()
	last := gridFloor(m.clock.Now(), routine.interval)
	for {
		select {
		case <-routine.ctx.Done():
			return
		case <-m.clock.After(last.Add(routine.interval).Sub(m.clock.Now())):
			if t := gridFloor(m.clock.Now(), routine.interval); t.After(last) {
				last = t
				m.evaluateInterval(routine.ctx, routine.interval, t)
			}
		}
	}
}

// evaluateInterval evaluates at t every recording rule whose group interval
// equals interval. Rules are re-read each cycle to pick up changes.
func (m *RuleManager) evaluateInterval(ctx context.Context, interval time.Duration, t time.Time) {
	groups, err := m.reader.GetAllRuleGroups(ctx)
	if err != nil {
		logger.Error("RuleManager: load rules for evaluation: ", err.Error())
		return
	}
	for namespace, gs := range groups {
		for _, g := range gs {
			d, err := time.ParseDuration(g.Interval)
			if err != nil || d != interval {
				continue
			}
			for _, rule := range g.Rules {
				if rule.IsRecording() {
					m.evaluateRecordingRule(namespace, g.Name, rule, t)
				}
			}
		}
	}
}

// evaluateRecordingRule evaluates one recording rule at t, writes the result
// back stamped at t and records its health. A failed evaluation or write-back
// records an error.
func (m *RuleManager) evaluateRecordingRule(namespace, groupName string, rule Rule, t time.Time) {
	start := time.Now()
	result, err := m.evaluator.Evaluate(m.ctx, rule.Expr, t)
	if err == nil {
		result, err = recordedVector(rule.Record, rule.Labels, result, t.UnixMilli())
	}
	key := ruleSeriesKey(namespace, groupName, rule)
	if err == nil {
		err = m.writer.Write(append(result, m.vanished(key, result, t.UnixMilli())...))
	}
	if err == nil {
		m.setLastSeries(key, result)
	}
	h := RuleHealth{Health: "ok", LastEvalTime: t, EvaluationTime: time.Since(start).Seconds()}
	if err != nil {
		h.Health, h.LastError = "err", err.Error()
		logger.Error("RuleManager: recording rule ", rule.Record, ": ", err.Error())
	}
	m.setRuleHealth(namespace, groupName, rule.Record, h)
}

// vanished returns a stale marker at ts for each label set the rule's last
// written result held and result lacks.
func (m *RuleManager) vanished(key string, result promql.Vector, ts int64) promql.Vector {
	m.lastSeriesMtx.Lock()
	defer m.lastSeriesMtx.Unlock()
	last := m.lastSeries[key]
	if len(last) == 0 {
		return nil
	}
	present := make(map[uint64]struct{}, len(result))
	for _, s := range result {
		present[s.Metric.Hash()] = struct{}{}
	}
	var markers promql.Vector
	for h, lbls := range last {
		if _, ok := present[h]; !ok {
			markers = append(markers, promql.Sample{Metric: lbls, T: ts, F: math.Float64frombits(value.StaleNaN)})
		}
	}
	return markers
}

func (m *RuleManager) setLastSeries(key string, result promql.Vector) {
	set := make(map[uint64]labels.Labels, len(result))
	for _, s := range result {
		set[s.Metric.Hash()] = s.Metric
	}
	m.lastSeriesMtx.Lock()
	m.lastSeries[key] = set
	m.lastSeriesMtx.Unlock()
}

// pruneLastSeries drops the last results of rules no longer in groups.
func (m *RuleManager) pruneLastSeries(groups NamespaceRuleGroups) {
	valid := make(map[string]struct{})
	for namespace, gs := range groups {
		for _, g := range gs {
			for _, rule := range g.Rules {
				if rule.IsRecording() {
					valid[ruleSeriesKey(namespace, g.Name, rule)] = struct{}{}
				}
			}
		}
	}
	m.lastSeriesMtx.Lock()
	defer m.lastSeriesMtx.Unlock()
	for k := range m.lastSeries {
		if _, ok := valid[k]; !ok {
			delete(m.lastSeries, k)
		}
	}
}

// ruleSeriesKey identifies a rule by its group, record name and labels; a rule
// whose expression changes keeps its last result.
func ruleSeriesKey(namespace, groupName string, rule Rule) string {
	return ruleHealthKey(namespace, groupName, rule.Record) + "\x00" + labels.FromMap(rule.Labels).String()
}

// GetPrometheusRules returns recording rules in the Prometheus API format,
// annotated with their last evaluation health.
func (m *RuleManager) GetPrometheusRules() []PrometheusGroup {
	groups, err := m.reader.GetAllRuleGroups(context.Background())
	if err != nil {
		logger.Error("RuleManager: fetch rules for API: ", err.Error())
		return []PrometheusGroup{}
	}

	promGroups := []PrometheusGroup{}
	for namespace, gs := range groups {
		for _, g := range gs {
			promRules := []PrometheusRule{}
			// Derive the group's evaluation status from its rules' actual
			// health rather than reporting a synthetic "now": the group's last
			// evaluation is the most recent evaluation among its rules (zero if
			// none has run yet), and its evaluation time is the sum of theirs.
			var groupLastEval time.Time
			var groupEvalTime float64
			for _, rule := range g.Rules {
				if !rule.IsRecording() {
					continue
				}
				health, lastErr, lastEval, evalTime := "unknown", "", time.Time{}, 0.0
				if h, ok := m.getRuleHealth(namespace, g.Name, rule.Record); ok {
					health, lastErr, lastEval, evalTime = h.Health, h.LastError, h.LastEvalTime, h.EvaluationTime
				}
				if lastEval.After(groupLastEval) {
					groupLastEval = lastEval
				}
				groupEvalTime += evalTime
				promRules = append(promRules, PrometheusRule{
					Name:           rule.Record,
					Query:          rule.Expr,
					Labels:         rule.Labels,
					Health:         health,
					LastError:      lastErr,
					Type:           "recording",
					LastEvaluation: lastEval.UTC().Format(time.RFC3339Nano),
					EvaluationTime: evalTime,
				})
			}
			if len(promRules) == 0 {
				continue
			}
			intervalSeconds := 60.0
			if d, err := time.ParseDuration(g.Interval); err == nil {
				intervalSeconds = d.Seconds()
			}
			promGroups = append(promGroups, PrometheusGroup{
				Name:           g.Name,
				File:           namespace,
				Rules:          promRules,
				Interval:       intervalSeconds,
				LastEvaluation: groupLastEval.UTC().Format(time.RFC3339Nano),
				EvaluationTime: groupEvalTime,
			})
		}
	}
	return promGroups
}

func (m *RuleManager) pollForChanges() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			groups, err := m.reader.GetAllRuleGroups(m.ctx)
			if err != nil {
				logger.Error("RuleManager: reload rules during poll: ", err.Error())
				continue
			}
			m.updateRoutines(groups)
		}
	}
}

func ruleHealthKey(namespace, groupName, ruleName string) string {
	return namespace + ":" + groupName + ":" + ruleName
}

func (m *RuleManager) setRuleHealth(namespace, groupName, ruleName string, h RuleHealth) {
	m.health.Store(ruleHealthKey(namespace, groupName, ruleName), h)
}

func (m *RuleManager) getRuleHealth(namespace, groupName, ruleName string) (RuleHealth, bool) {
	v, ok := m.health.Load(ruleHealthKey(namespace, groupName, ruleName))
	if !ok {
		return RuleHealth{}, false
	}
	return v.(RuleHealth), true
}
