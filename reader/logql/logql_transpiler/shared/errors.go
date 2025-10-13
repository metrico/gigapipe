package shared

import (
	"fmt"
	"runtime/debug"

	"github.com/metrico/qryn/v4/reader/utils/logger"
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
