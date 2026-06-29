package ruler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/metrico/qryn/v4/writer/utils/logger"
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

// intervalRoutine evaluates all rules sharing one interval on its own ticker.
type intervalRoutine struct {
	interval time.Duration
	ticker   *time.Ticker
	ctx      context.Context
	cancel   context.CancelFunc
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

	routines    map[time.Duration]*intervalRoutine
	routinesMtx sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	pollInterval time.Duration
}

// NewRuleManager builds a manager from its dependencies.
func NewRuleManager(evaluator RuleEvaluator, reader RuleReader, writer RecordingRuleWriter, pollInterval time.Duration) *RuleManager {
	return &RuleManager{
		evaluator:    evaluator,
		reader:       reader,
		writer:       writer,
		routines:     make(map[time.Duration]*intervalRoutine),
		pollInterval: pollInterval,
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
		routine.ticker.Stop()
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
			if err != nil {
				logger.Error("RuleManager: skipping group with invalid interval ", g.Name, ": ", err.Error())
				continue
			}
			intervals[d] = true
		}
	}

	m.routinesMtx.Lock()
	defer m.routinesMtx.Unlock()

	for interval, routine := range m.routines {
		if !intervals[interval] {
			routine.cancel()
			routine.ticker.Stop()
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
			ticker:   time.NewTicker(interval),
			ctx:      ctx,
			cancel:   cancel,
		}
		m.routines[interval] = routine
		m.wg.Add(1)
		go m.runIntervalRoutine(routine)
	}
}

func (m *RuleManager) runIntervalRoutine(routine *intervalRoutine) {
	defer m.wg.Done()
	for {
		select {
		case <-routine.ctx.Done():
			return
		case <-routine.ticker.C:
			m.evaluateInterval(routine.ctx, routine.interval)
		}
	}
}

// evaluateInterval evaluates every recording rule whose group interval equals
// interval. Rules are re-read each cycle to pick up changes.
func (m *RuleManager) evaluateInterval(ctx context.Context, interval time.Duration) {
	groups, err := m.reader.GetAllRuleGroups(ctx)
	if err != nil {
		logger.Error("RuleManager: load rules for evaluation: ", err.Error())
		return
	}
	now := time.Now().UTC()
	for namespace, gs := range groups {
		for _, g := range gs {
			d, err := time.ParseDuration(g.Interval)
			if err != nil || d != interval {
				continue
			}
			for _, rule := range g.Rules {
				if rule.IsRecording() {
					m.evaluateRecordingRule(namespace, g.Name, rule, now)
				}
			}
		}
	}
}

// evaluateRecordingRule evaluates one recording rule, records its health, and
// writes the result back. A failed evaluation records an error and writes
// nothing.
func (m *RuleManager) evaluateRecordingRule(namespace, groupName string, rule Rule, now time.Time) {
	start := time.Now()
	result, err := m.evaluator.Evaluate(m.ctx, rule.Expr, now)
	dur := time.Since(start)
	if err != nil {
		m.setRuleHealth(namespace, groupName, rule.Record, RuleHealth{
			Health:         "err",
			LastError:      err.Error(),
			LastEvalTime:   now,
			EvaluationTime: dur.Seconds(),
		})
		logger.Error("RuleManager: evaluate recording rule ", rule.Record, ": ", err.Error())
		return
	}
	m.setRuleHealth(namespace, groupName, rule.Record, RuleHealth{
		Health:         "ok",
		LastEvalTime:   now,
		EvaluationTime: dur.Seconds(),
	})

	if err := m.writer.Write(rule.Record, rule.Labels, result); err != nil {
		logger.Error("RuleManager: write back recording rule ", rule.Record, ": ", err.Error())
	}
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
