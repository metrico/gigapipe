package dsn

import (
	"context"
	"fmt"
	"time"
)

// WithLimit is parent cancelled after d, with no deadline of its own. The
// context's cause names the limit.
func WithLimit(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	timer := time.AfterFunc(d, func() { cancel(LimitReached(d)) })
	return ctx, func() {
		timer.Stop()
		cancel(nil)
	}
}

// LimitReached is the cause of a context ended by a time limit of d.
func LimitReached(d time.Duration) error {
	return fmt.Errorf("time limit %v reached", d)
}
