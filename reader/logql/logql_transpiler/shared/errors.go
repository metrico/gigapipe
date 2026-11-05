package shared

import (
	"fmt"
	"runtime/debug"

	"github.com/metrico/qryn/v5/reader/utils/logger"
)

type NotSupportedError struct {
	Msg string
}

func (n *NotSupportedError) Error() string {
	return n.Msg
}

func TamePanic(out chan []LogEntry) {
	if err := recover(); err != nil {
		logger.Error(err, " stack:", string(debug.Stack()))
		out <- []LogEntry{{Err: fmt.Errorf("panic: %v", err)}}
		recover()
	}
}

// Drain discards whatever is left in ch in the background, so the goroutine
// feeding it can finish and release what it holds (e.g. a ClickHouse
// connection) once its consumer stops reading early.
func Drain(ch chan []LogEntry) {
	go func() {
		for range ch {
		}
	}()
}
