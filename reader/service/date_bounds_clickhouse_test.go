package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	clconfig "github.com/metrico/cloki-config"
	baseconfig "github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/reader/config"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/utils/zonetest"
)

// dateBoundsDB is recreated on the GRID_TEST_CLICKHOUSE server (an HTTP URL
// with credentials); the test skips when it is unset.
const dateBoundsDB = "reader_date_bounds_test"

type chSession struct{ db *sql.DB }

func (s chSession) GetName() string { return dateBoundsDB }
func (s chSession) QueryCtx(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, q, args...)
}
func (s chSession) ExecCtx(ctx context.Context, q string, args ...any) error {
	_, err := s.db.ExecContext(ctx, q, args...)
	return err
}
func (s chSession) Conn(ctx context.Context) (*sql.Conn, error) { return s.db.Conn(ctx) }
func (s chSession) Begin() (*sql.Tx, error)                     { return s.db.Begin() }
func (s chSession) Close()                                      { s.db.Close() }

type chRegistry struct{ conn *model.DataDatabasesMap }

func (r chRegistry) GetDB(context.Context) (*model.DataDatabasesMap, error) { return r.conn, nil }
func (r chRegistry) Run()                                                   {}
func (r chRegistry) Stop()                                                  {}
func (r chRegistry) Ping() error                                            { return nil }

func openDateBoundsDB(t *testing.T) *model.DataDatabasesMap {
	t.Helper()
	raw := os.Getenv("GRID_TEST_CLICKHOUSE")
	if raw == "" {
		t.Skip("GRID_TEST_CLICKHOUSE is not set")
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	pass, _ := u.User.Password()
	open := func(db string) *sql.DB {
		return clickhouse.OpenDB(&clickhouse.Options{Addr: []string{u.Host}, Protocol: clickhouse.HTTP,
			Auth: clickhouse.Auth{Database: db, Username: u.User.Username(), Password: pass}})
	}
	admin := open("default")
	defer admin.Close()
	for _, q := range []string{"DROP DATABASE IF EXISTS " + dateBoundsDB, "CREATE DATABASE " + dateBoundsDB} {
		if _, err := admin.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	db := open(dateBoundsDB)
	t.Cleanup(func() { db.Close() })
	return &model.DataDatabasesMap{Config: &baseconfig.ClokiBaseDataBase{}, Session: chSession{db}}
}

// Stored dates around the request (2026-03-10 12:00, 2026-03-11 02:00] UTC:
// 03-09 to 03-12 lie within the bounds, 03-08 and 03-13 a day beyond them.
var dateBoundsSeed = []string{
	`CREATE TABLE time_series (date Date, fingerprint UInt64, labels String, name String, type UInt8)
		ENGINE = ReplacingMergeTree ORDER BY (fingerprint, type)`,
	`CREATE TABLE time_series_gin (date Date, key String, val String, fingerprint UInt64, type UInt8)
		ENGINE = ReplacingMergeTree ORDER BY (key, val, fingerprint, type)`,
	`CREATE TABLE settings (fingerprint UInt64, type String, name String, value String,
		inserted_at DateTime64(9, 'UTC')) ENGINE = ReplacingMergeTree(inserted_at) ORDER BY fingerprint`,
	`CREATE TABLE profiles_series (date Date, type_id LowCardinality(String),
		sample_types_units Array(Tuple(String, String)), service_name LowCardinality(String),
		fingerprint UInt64, tags Array(Tuple(String, String)))
		ENGINE = ReplacingMergeTree ORDER BY (date, type_id, fingerprint)`,
	`INSERT INTO time_series VALUES
		('2026-03-09', 1, '{"job":"a","pod":"behind","k_behind":"1"}', '', 0),
		('2026-03-11', 2, '{"job":"a","pod":"next","k_next":"1"}', '', 0),
		('2026-03-08', 3, '{"job":"a","pod":"before","k_before":"1"}', '', 0),
		('2026-03-13', 4, '{"job":"a","pod":"after","k_after":"1"}', '', 0),
		('2026-03-12', 5, '{"job":"a","pod":"ahead","k_ahead":"1"}', '', 0)`,
	`INSERT INTO time_series_gin SELECT date, kv.1, kv.2, fingerprint, type FROM time_series
		ARRAY JOIN JSONExtractKeysAndValues(labels, 'String') AS kv`,
	`INSERT INTO profiles_series VALUES
		('2026-03-09', 'behind:cpu:nanoseconds', [('cpu', 'nanoseconds')], 's', 1, []),
		('2026-03-11', 'next:cpu:nanoseconds', [('cpu', 'nanoseconds')], 's', 2, []),
		('2026-03-08', 'before:cpu:nanoseconds', [('cpu', 'nanoseconds')], 's', 3, []),
		('2026-03-13', 'after:cpu:nanoseconds', [('cpu', 'nanoseconds')], 's', 4, []),
		('2026-03-12', 'ahead:cpu:nanoseconds', [('cpu', 'nanoseconds')], 's', 5, [])`,
}

// collect returns the sorted data entries of a label service response,
// taking the pod label of each object entry.
func collect(t *testing.T, ch chan string, err error) []string {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for s := range ch {
		b.WriteString(s)
	}
	var res struct{ Data []json.RawMessage }
	if err := json.Unmarshal([]byte(b.String()), &res); err != nil {
		t.Fatalf("%v: %s", err, b.String())
	}
	var out []string
	for _, d := range res.Data {
		var s string
		if json.Unmarshal(d, &s) != nil {
			var m map[string]string
			if err := json.Unmarshal(d, &m); err != nil {
				t.Fatal(err)
			}
			s = m["pod"]
		}
		out = append(out, s)
	}
	slices.Sort(out)
	return out
}

// TestReadsDoNotDependOnTheReaderZone runs the reads in several reader zones
// against fixed instants: each finds the rows stored a day either side of the
// request's UTC dates, and none of those further out.
func TestReadsDoNotDependOnTheReaderZone(t *testing.T) {
	conn := openDateBoundsDB(t)
	ctx := context.Background()
	for _, q := range dateBoundsSeed {
		if err := conn.Session.ExecCtx(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	if config.Cloki == nil {
		config.Cloki = clconfig.New(clconfig.CLOKI_READER, nil, "", "")
	}
	from := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	to := time.Date(2026, 3, 11, 2, 0, 0, 0, time.UTC)
	fromMs, toMs := from.UnixMilli(), to.UnixMilli()
	labelsSvc := NewQueryLabelsService(&model.ServiceData{Session: chRegistry{conn}})
	profSvc := &ProfService{DataSession: chRegistry{conn}}

	zones := []string{"UTC", "Asia/Tokyo", "America/Los_Angeles"}
	zonetest.Run(t, zones, func(t *testing.T, _ *time.Location) {
		check := func(what string, got, want []string) {
			t.Helper()
			if !slices.Equal(got, want) {
				t.Errorf("%s: got %v, want %v", what, got, want)
			}
		}

		ch, err := labelsSvc.Labels(ctx, fromMs, toMs, 1, nil)
		check("label names", collect(t, ch, err), []string{"job", "k_ahead", "k_behind", "k_next", "pod"})
		ch, err = labelsSvc.Labels(ctx, fromMs, toMs, 1, []string{`{job="a"}`})
		check("label names of {job=a}", collect(t, ch, err), []string{"job", "k_ahead", "k_behind", "k_next", "pod"})
		ch, err = labelsSvc.Values(ctx, "pod", nil, fromMs, toMs, 1)
		check("pod values", collect(t, ch, err), []string{"ahead", "behind", "next"})
		ch, err = labelsSvc.Values(ctx, "pod", []string{`{job="a"}`}, fromMs, toMs, 1)
		check("pod values of {job=a}", collect(t, ch, err), []string{"ahead", "behind", "next"})
		ch, err = labelsSvc.Series(ctx, []string{`{job="a"}`}, fromMs, toMs, 1)
		check("series of {job=a}", collect(t, ch, err), []string{"ahead", "behind", "next"})

		types, err := profSvc.ProfileTypes(ctx, time.UnixMilli(fromMs), time.UnixMilli(toMs))
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, tp := range types {
			names = append(names, tp.Name)
		}
		slices.Sort(names)
		check("profile types", names, []string{"ahead", "behind", "next"})

		l := newLabelsGetter(time.UnixMilli(fromMs), time.UnixMilli(toMs), conn, ctx)
		for fp := uint64(1); fp <= 5; fp++ {
			l.Plan(fp)
		}
		if err := l.Fetch(); err != nil {
			t.Fatal(err)
		}
		var pods []string
		for fp := uint64(1); fp <= 5; fp++ {
			for _, lb := range l.Get(fp) {
				if lb.Name == "pod" {
					pods = append(pods, lb.Value)
				}
			}
		}
		slices.Sort(pods)
		check("prometheus labels", pods, []string{"ahead", "behind", "next"})
	})
}
