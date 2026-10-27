package logql_transpiler

import (
	"context"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

func TestFixPeriodRecoversPanic(t *testing.T) {
	in, done := feedUnbuffered(3)
	m := &FixPeriodPlanner{Main: &chanProcessor{in}, Duration: time.Second}
	// A zero step makes the bucket index computation divide by zero.
	ctx := &shared.PlannerContext{Ctx: context.Background(), From: time.Unix(0, 0), To: time.Unix(60, 0)}
	out, err := m.Process(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	assertPanicRecovered(t, out, done)
}
