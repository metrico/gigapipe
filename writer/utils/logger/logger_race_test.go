package logger

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"testing"
)

func TestLoggerRaceCond(t *testing.T) {
	Logger.SetFormatter(&logrus.JSONFormatter{})
	qrynFmt := &qrynFormatter{
		formatter: Logger.Formatter,
		url:       "",
		app:       "",
		hostname:  "a",
		headers:   nil,
	}
	qrynFmt.Run()
	Logger.SetFormatter(qrynFmt)
	g := errgroup.Group{}
	for range 10 {
		g.Go(func() error {
			for range 100000 {
				Logger.Info("a", "B", fmt.Errorf("aaaa"))
			}
			return nil
		})
	}
	g.Wait()
}
