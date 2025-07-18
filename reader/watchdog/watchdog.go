package watchdog

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/utils/logger"
)

var svc *model.ServiceData
var retries = 0
var lastSuccessfulCheck = time.Now()

func Init(_svc *model.ServiceData) {
	svc = _svc
	ticker := time.NewTicker(time.Second * 5)
	go func() {
		for range ticker.C {
			err := svc.Ping()
			if err == nil {
				retries = 0
				lastSuccessfulCheck = time.Now()
				logger.Info("---- WATCHDOG CHECK OK ----")
				continue
			}
			retries++
			logger.Info("---- WATCHDOG REPORT ----")
			logger.Error("database not responding ", retries*5, " seconds")
			if retries > 5 {
				panic("WATCHDOG PANIC: database not responding")
			}
		}
	}()
}

func Check() error {
	if lastSuccessfulCheck.Add(time.Second * 30).After(time.Now()) {
		return nil
	}
	return fmt.Errorf("database not responding since %v", lastSuccessfulCheck)
}
