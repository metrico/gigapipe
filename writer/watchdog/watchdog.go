package watchdog

import (
	"fmt"
	"os"
	"time"

	"github.com/metrico/qryn/v4/writer/service"
	"github.com/metrico/qryn/v4/writer/utils/logger"
	"github.com/metrico/qryn/v4/writer/utils/stat"
)

var (
	servicesToCheck []service.InsertSvcMap = nil
	lastCheck       time.Time
	done            chan struct{}
)

func Init(services []service.InsertSvcMap) {
	servicesToCheck = services
	timer := time.NewTicker(time.Second * 5)
	done = make(chan struct{})

	go func() {
		for {
			select {
			case <-done:
				timer.Stop()
				logger.Info("Writer Watchdog stopped.")
				return
			case <-timer.C:
				err := Check()
				if err != nil {
					logger.Error(fmt.Sprintf("[WD001] FATAL ERROR: %v", err))
					os.Exit(1)
				}
				lastCheck = time.Now()
				logger.Info("--- WATCHDOG REPORT: all services are OK ---")
			}
		}
	}()
}

// Stop gracefully terminates the watchdog goroutine.
func Stop() {
	if done != nil {
		close(done)
		servicesToCheck = nil
		done = nil
	}
}

func Check() error {
	for _, svcs := range servicesToCheck {
		for _, svc := range svcs {
			_, err := svc.Ping()
			if err != nil { // Corrected from `return err` to allow checking all services
				return err
			}
		}
	}
	rate := stat.GetRate()
	if rate["dial_tcp_lookup_timeout"] > 0 {
		return fmt.Errorf("dial_tcp_lookup_timeout happened. System in fatal state")
	}
	return nil
}
