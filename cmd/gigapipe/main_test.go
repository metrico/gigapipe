package main

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/shared/metricretention"
	writergrpc "github.com/metrico/qryn/v5/writer/grpc"
)

// stepNames renders the boot sequence for a mode as an ordered slice of names.
func stepNames(mode string) []string {
	seq := bootSequence(mode)
	names := make([]string, len(seq))
	for i, s := range seq {
		names[i] = s.name
	}
	return names
}

// orderViolations reports every init-ordering invariant start() depends on that
// the given sequence breaks. Empty result means the order is safe.
//
// Two independent constraints pull in opposite directions, so both must hold at
// once:
//
//   - reader BEFORE ruler: reader.Init populates the reader registry the ruler
//     binds its rule sessions to; ordering reader after ruler yields a nil
//     session (the regression that shipped on alpha via #864).
//   - view LAST: view registers a wildcard "/" catch-all route that shadows any
//     route registered after it; ordering view before ruler hides the ruler's
//     HTTP routes (the regression that lived on master).
//
// The safe order writer -> reader -> ruler -> view is the only one that
// satisfies both: reader ahead of ruler, view still dead last.
func orderViolations(names []string) []string {
	writer := slices.Index(names, "writer")
	reader := slices.Index(names, "reader")
	ruler := slices.Index(names, "ruler")
	view := slices.Index(names, "view")

	// Constraints below only apply to subsystems actually present in the mode.
	var v []string
	// The ruler's write-back uses the writer's ClickHouse client.
	if writer != -1 && ruler != -1 && writer > ruler {
		v = append(v, "writer must init before ruler")
	}
	// reader.Init populates the reader registry the ruler binds rule sessions to.
	if reader != -1 && ruler != -1 && reader > ruler {
		v = append(v, "reader must init before ruler (else the ruler binds a nil session)")
	}
	// view's wildcard "/" route must be registered after everything else.
	if view != -1 && view != len(names)-1 {
		v = append(v, "view must init last (its catch-all route shadows anything after it)")
	}
	return v
}

// TestBootSequenceOrder pins the subsystem init ordering start() depends on: the
// real sequence for every combined mode must break none of the invariants.
func TestBootSequenceOrder(t *testing.T) {
	for _, mode := range []string{"all", ""} {
		t.Run("mode="+mode, func(t *testing.T) {
			names := stepNames(mode)
			if len(names) != 4 {
				t.Fatalf("mode %q expected 4 subsystems, got %v", mode, names)
			}
			if got := orderViolations(names); len(got) != 0 {
				t.Errorf("mode %q order %v violates: %v", mode, names, got)
			}
		})
	}
}

// TestBootSequenceOrderCatchesKnownRegressions guards the guard: it feeds the two
// historical bad orderings through the same invariant check and asserts each is
// still detected, so neither regression can silently return.
func TestBootSequenceOrderCatchesKnownRegressions(t *testing.T) {
	cases := []struct {
		name  string
		order []string
		want  string // substring the matching violation must contain
	}{
		{
			name:  "reader-after-ruler (alpha #864 nil session)",
			order: []string{"writer", "ruler", "reader", "view"},
			want:  "reader must init before ruler",
		},
		{
			name:  "view-before-ruler (master wildcard shadows routes)",
			order: []string{"writer", "reader", "view", "ruler"},
			want:  "view must init last",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := orderViolations(c.order)
			if len(got) == 0 {
				t.Fatalf("order %v should have been rejected, got no violations", c.order)
			}
			if !slices.ContainsFunc(got, func(s string) bool { return strings.Contains(s, c.want) }) {
				t.Errorf("order %v: expected a violation containing %q, got %v", c.order, c.want, got)
			}
		})
	}
}

// TestBootSequenceModesScope guards the per-mode gating: the ruler only runs in
// the combined modes, and reader-only / writer-only modes stay minimal.
func TestBootSequenceModesScope(t *testing.T) {
	if got := stepNames("reader"); !slices.Equal(got, []string{"reader", "view"}) {
		t.Errorf(`mode "reader" should be [reader view], got %v`, got)
	}
	if got := stepNames("writer"); !slices.Equal(got, []string{"writer"}) {
		t.Errorf(`mode "writer" should be [writer], got %v`, got)
	}
	if slices.Contains(stepNames("reader"), "ruler") {
		t.Error(`mode "reader" must not start the ruler`)
	}
	if slices.Contains(stepNames("writer"), "ruler") {
		t.Error(`mode "writer" must not start the ruler`)
	}
}

// nopHandler stands in for the mux router in httpRoot tests. It records
// whether it was reached, so a test can assert that gRPC traffic was
// intercepted rather than passed through. It is a pointer type so the
// returned handler can also be compared by identity: func values (and so
// http.HandlerFunc) are not comparable in Go.
type nopHandler struct{ served bool }

func (h *nopHandler) ServeHTTP(http.ResponseWriter, *http.Request) { h.served = true }

// grpcRequest builds the request shape Mux dispatches on: HTTP/2 carrying a
// gRPC content type, on a real OTLP method path.
func grpcRequest() *http.Request {
	r := httptest.NewRequest(http.MethodPost,
		"/opentelemetry.proto.collector.trace.v1.TraceService/Export", http.NoBody)
	r.ProtoMajor, r.ProtoMinor, r.Proto = 2, 0, "HTTP/2.0"
	r.Header.Set("Content-Type", "application/grpc")
	return r
}

// TestServesGRPCModes pins the OTLP/gRPC receiver's mode gate to literal
// expectations. servesGRPC derives its answer from bootSequence, so asserting
// against bootSequence again would be a tautology; literals are what catch a
// change to bootSequence — a renamed step, a dropped mode — that would
// silently stop mounting the receiver on a node that ingests.
func TestServesGRPCModes(t *testing.T) {
	want := map[string]bool{
		"all": true, "writer": true, "": true,
		"reader": false, "init_only": false,
	}
	for mode, w := range want {
		if got := servesGRPC(mode); got != w {
			t.Errorf("servesGRPC(%q) = %v, want %v", mode, got, w)
		}
	}
}

// TestHTTPRootWriterModes asserts that a node which serves gRPC gets both
// halves: the dispatcher wrapping the router, and the protocol set that
// carries it. Cleartext HTTP/2 must be enabled, since gRPC rides
// prior-knowledge h2c on this port.
func TestHTTPRootWriterModes(t *testing.T) {
	for _, mode := range []string{"all", "writer", ""} {
		t.Run("mode="+mode, func(t *testing.T) {
			base := &nopHandler{}
			root, protocols := httpRoot(base, mode, writergrpc.Options{})
			root.ServeHTTP(httptest.NewRecorder(), grpcRequest())
			if base.served {
				t.Error("a gRPC request reached the HTTP router: it must be intercepted by the OTLP/gRPC dispatcher")
			}
			if protocols == nil {
				t.Fatal("expected a protocol set enabling cleartext HTTP/2, got nil")
			}
			if !protocols.UnencryptedHTTP2() {
				t.Error("cleartext HTTP/2 must be enabled: gRPC rides prior-knowledge h2c on this port")
			}
			if !protocols.HTTP1() {
				t.Error("HTTP/1 must stay enabled: OTLP/HTTP and every other route share this port")
			}
		})
	}
}

// TestHTTPRootReaderModeLeavesProtocolsDefault is the negative half, and the
// reason httpRoot returns both values from one gate. A reader-only node has no
// write path, so it mounts no gRPC handler — and must therefore not advertise
// cleartext HTTP/2 either. Setting Protocols unconditionally would enable h2c
// on readers with nothing behind it, widening the protocols they accept for no
// gain. A nil result leaves net/http's default, which is what readers served
// before the receiver existed.
func TestHTTPRootReaderModeLeavesProtocolsDefault(t *testing.T) {
	base := &nopHandler{}
	root, protocols := httpRoot(base, "reader", writergrpc.Options{})
	if root != http.Handler(base) {
		t.Error("reader-only nodes must serve the router unwrapped, with no gRPC dispatcher")
	}
	if protocols != nil {
		t.Errorf("reader-only nodes must leave Protocols at net/http's default, got %+v", protocols)
	}
}

func TestStartRejectsACoarserTierShorterThanAFinerOne(t *testing.T) {
	t.Setenv("METRICS_5M_DAYS", "3")
	cfg := clconfig.New(clconfig.CLOKI_READER, nil, "", "")
	if err := portEnv(cfg); err == nil || !strings.Contains(err.Error(), "METRICS_5M_DAYS") {
		t.Errorf("portEnv = %v, want a METRICS_5M_DAYS error", err)
	}
}

func TestStartRejectsAnUnknownReadTier(t *testing.T) {
	t.Setenv("METRICS_READ_TIER", "15s")
	cfg := clconfig.New(clconfig.CLOKI_READER, nil, "", "")
	if err := portEnv(cfg); err == nil || !strings.Contains(err.Error(), "METRICS_READ_TIER") {
		t.Errorf("portEnv = %v, want a METRICS_READ_TIER error", err)
	}
}

func schemaStepNames(t *testing.T, mode string, omitCreateTables string) []string {
	t.Helper()
	t.Setenv("OMIT_CREATE_TABLES", omitCreateTables)
	steps, err := schemaSteps(mode)
	if err != nil {
		t.Fatalf("mode %q, OMIT_CREATE_TABLES=%q: %v", mode, omitCreateTables, err)
	}
	var names []string
	for _, s := range steps {
		names = append(names, s.name)
	}
	return names
}

func TestSchemaStepsRunTheMetricImportAfterInitAndRotate(t *testing.T) {
	for _, mode := range []string{"all", "writer"} {
		if got := schemaStepNames(t, mode, ""); !slices.Equal(got, []string{"init", "rotate", "import"}) {
			t.Errorf("mode %q: schema steps %v, want [init rotate import]", mode, got)
		}
	}
	if got := schemaStepNames(t, "init_only", ""); !slices.Equal(got, []string{"init", "rotate"}) {
		t.Errorf(`mode "init_only": schema steps %v, want [init rotate]`, got)
	}
	if got := schemaStepNames(t, "reader", ""); len(got) != 0 {
		t.Errorf(`mode "reader": schema steps %v, want none`, got)
	}
}

func TestOmitCreateTablesSkipsEverySchemaStep(t *testing.T) {
	for _, mode := range []string{"all", "writer", "init_only"} {
		if got := schemaStepNames(t, mode, "true"); len(got) != 0 {
			t.Errorf("mode %q with OMIT_CREATE_TABLES: schema steps %v, want none", mode, got)
		}
	}
}

func TestOmitCreateTablesIsReadOnlyInASchemaMode(t *testing.T) {
	if got := schemaStepNames(t, "reader", "maybe"); len(got) != 0 {
		t.Errorf(`mode "reader": schema steps %v, want none`, got)
	}
	t.Setenv("OMIT_CREATE_TABLES", "maybe")
	if _, err := schemaSteps("writer"); err == nil {
		t.Error(`mode "writer" accepted OMIT_CREATE_TABLES=maybe`)
	}
}

// configWithDatabases returns a config file's settings holding one database per TTL.
func configWithDatabases(t *testing.T, ttlDays ...int) *clconfig.ClokiConfig {
	t.Helper()
	prev := metricretention.Configured()
	t.Cleanup(func() { metricretention.Configure(prev) })
	cfg := clconfig.New(clconfig.CLOKI_READER, nil, "", "")
	for _, d := range ttlDays {
		cfg.Setting.DATABASE_DATA = append(cfg.Setting.DATABASE_DATA, config.ClokiBaseDataBase{TTLDays: d})
	}
	return cfg
}

func TestStartResolvesTheMetricTiersOfEachDatabaseAndTheForcedTier(t *testing.T) {
	t.Setenv("METRICS_1H_DAYS", "400")
	t.Setenv("METRICS_15S_TTL_DAYS", "9")
	t.Setenv("METRICS_READ_TIER", "5m")
	cfg := configWithDatabases(t, 3, 60)
	if err := portEnv(cfg); err != nil {
		t.Fatal(err)
	}
	s := metricretention.Configured()
	if s.ReadTier != "5m" || s.Rollup(3) != 9 {
		t.Errorf("ReadTier = %q, Rollup = %d, want 5m and 9", s.ReadTier, s.Rollup(3))
	}
	for i, want := range []metricretention.Tiers{
		{RawDays: 3, FiveMinuteDays: 30, HourDays: 400},
		{RawDays: 60, FiveMinuteDays: 60, HourDays: 400},
	} {
		if got, err := s.Tiers(cfg.Setting.DATABASE_DATA[i].TTLDays); err != nil || got != want {
			t.Errorf("database %d: tiers = %+v, %v, want %+v", i, got, err, want)
		}
	}
}

func TestStartTakesTheRawTierFromSamplesDays(t *testing.T) {
	t.Setenv("SAMPLES_DAYS", "12")
	cfg := configWithDatabases(t)
	if err := portEnv(cfg); err != nil {
		t.Fatal(err)
	}
	want := metricretention.Tiers{RawDays: 12, FiveMinuteDays: 30, HourDays: 365}
	if got, err := metricretention.Configured().Tiers(cfg.Setting.DATABASE_DATA[0].TTLDays); err != nil || got != want {
		t.Errorf("tiers = %+v, %v, want %+v", got, err, want)
	}
}

func TestStartRejectsTiersThatBreakTheRuleForOneDatabase(t *testing.T) {
	t.Setenv("METRICS_5M_DAYS", "20")
	if err := portEnv(configWithDatabases(t, 7, 30)); err == nil || !strings.Contains(err.Error(), "METRICS_5M_DAYS") {
		t.Errorf("portEnv = %v, want a METRICS_5M_DAYS error", err)
	}
}

func TestStartRejectsABadRollupLifetime(t *testing.T) {
	t.Setenv("METRICS_15S_TTL_DAYS", "off")
	if err := portEnv(configWithDatabases(t)); err == nil || !strings.Contains(err.Error(), "METRICS_15S_TTL_DAYS") {
		t.Errorf("portEnv = %v, want a METRICS_15S_TTL_DAYS error", err)
	}
}
