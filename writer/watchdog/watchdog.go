package watchdog

import (
	"fmt"
	"os"
	"time"

	"github.com/metrico/qryn/v4/writer/service"
	"github.com/metrico/qryn/v4/writer/utils/logger"
	"github.com/metrico/qryn/v4/writer/utils/stat"
)

var servicesToCheck []service.InsertSvcMap = nil
var lastCheck time.Time

func Init(services []service.InsertSvcMap) {
	servicesToCheck = services
	timer := time.NewTicker(time.Second * 5)
	go func() {
		for range timer.C {
			err := Check()
			if err != nil {
				logger.Error(fmt.Sprintf("[WD001] FATAL ERROR: %v", err))
				os.Exit(1)
			}
			lastCheck = time.Now()
			logger.Info("--- WATCHDOG REPORT: all services are OK ---")
		}
	}()
}

func Check() error {
	for _, svcs := range servicesToCheck {
		for _, svc := range svcs {
			_, err := svc.Ping()
			return err
		}
	}
	rate := stat.GetRate()
	if rate["dial_tcp_lookup_timeout"] > 0 {
		return fmt.Errorf("dial_tcp_lookup_timeout happened. System in fatal state")
	}
	return nil
}

func FastCheck() error {
	if lastCheck.Add(time.Second * 5).After(time.Now()) {
		return nil
	}
	return Check()
}
