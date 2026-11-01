package maintenance

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/sirupsen/logrus"
)

// fakeExec is a scripted Executor. It keeps the settings records in memory and logs every
// statement; on answers the others with a result (written rows, a scalar or rows) or an error.
type fakeExec struct {
	mu      sync.Mutex
	records *memRecords
	log     []Statement
	on      func(ctx context.Context, s Statement) (any, error)
}

func newFakeExec(server *testClock) *fakeExec {
	return &fakeExec{records: newMemRecords(server)}
}

// record sets a record as if an earlier run had written it.
func (f *fakeExec) record(name, value string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_ = f.records.put(context.Background(), name, value)
}

// value reads a record.
func (f *fakeExec) value(name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.records.values[name]
}

// statements returns the logged statements.
func (f *fakeExec) statements() []Statement {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Statement(nil), f.log...)
}

func (f *fakeExec) answer(ctx context.Context, s Statement) (any, error) {
	if f.on == nil {
		return nil, nil
	}
	return f.on(ctx, s)
}

func (f *fakeExec) Exec(ctx context.Context, s Statement) (uint64, error) {
	f.mu.Lock()
	f.log = append(f.log, s)
	if s.Op == opRecord {
		_ = f.records.put(ctx, s.Args[2].(string), s.Args[3].(string))
	}
	f.mu.Unlock()
	v, err := f.answer(ctx, s)
	if n, ok := v.(int); ok {
		return uint64(n), err
	}
	return 0, err
}

func (f *fakeExec) Scalar(ctx context.Context, s Statement, dest ...any) error {
	if s.Op == opRecordAge {
		f.mu.Lock()
		defer f.mu.Unlock()
		for name := range f.records.at {
			if migrationRecordFingerprint(name) == s.Args[0].(uint32) {
				age, _ := f.records.age(ctx, name)
				*dest[0].(*int64) = age.Milliseconds()
			}
		}
		return nil
	}
	f.mu.Lock()
	f.log = append(f.log, s)
	f.mu.Unlock()
	v, err := f.answer(ctx, s)
	if err != nil || v == nil {
		return err
	}
	reflect.ValueOf(dest[0]).Elem().Set(reflect.ValueOf(v).Convert(reflect.TypeOf(dest[0]).Elem()))
	return nil
}

func (f *fakeExec) Rows(ctx context.Context, s Statement, row func(scan func(dest ...any) error) error) error {
	var rows [][]any
	if s.Op == opRecords {
		f.mu.Lock()
		for name, value := range f.records.values {
			rows = append(rows, []any{name, value})
		}
		f.mu.Unlock()
	} else {
		f.mu.Lock()
		f.log = append(f.log, s)
		f.mu.Unlock()
		v, err := f.answer(ctx, s)
		if err != nil {
			return err
		}
		rows, _ = v.([][]any)
	}
	for _, r := range rows {
		err := row(func(dest ...any) error {
			if len(dest) != len(r) {
				return fmt.Errorf("scan into %d columns of %d", len(dest), len(r))
			}
			for i := range dest {
				reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(r[i]))
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// step names a statement: a record write by name and value, any other by op and table.
func step(s Statement) string {
	if s.Op == opRecord {
		return fmt.Sprintf("record %s=%s", s.Args[2], s.Args[3])
	}
	return s.Op + " " + s.Table
}

func quietLogger() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}
