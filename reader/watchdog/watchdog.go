package watchdog

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/v4/logger"
	"github.com/metrico/qryn/v4/reader/model"
)

var (
	svc                 *model.ServiceData
	retries             = 0
	lastSuccessfulCheck = time.Now()
	ticker              *time.Ticker
	done                chan struct{}
)

func Init(_svc *model.ServiceData) {
	svc = _svc
	ticker = time.NewTicker(time.Second * 5)
	done = make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				ticker.Stop()
				logger.Info("---- WATCHDOG STOPPED ----")
				return
			case <-ticker.C:
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
		}
	}()
}

func Stop() {
	if done != nil {
		done <- struct{}{}
	}
}

func Check() error {
	if lastSuccessfulCheck.Add(time.Second * 30).After(time.Now()) {
		return nil
	}
	return fmt.Errorf("database not responding since %v", lastSuccessfulCheck)
}
