package chwrapper

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/metrico/cloki-config/config"
)

// SmartDatabaseAdapter combines GeneralPurposeClient and WriteClient
type SmartDatabaseAdapter struct {
	Xdsn                 string
	DSN                  string
	dbObject             *config.ClokiBaseDataBase
	database             bool
	generalPurposeClient IChClient
	writeClient          IChClient
	mu                   sync.Mutex // To handle concurrency for close
	onceGeneralClient    sync.Once
	onceWriteClient      sync.Once
	initErr              error // Store initialization errors
}

// initGeneralClient initializes the general purpose client once
func (a *SmartDatabaseAdapter) initGeneralClient(ctx context.Context) error {
	a.onceGeneralClient.Do(func() {
		var err error
		if a.dbObject != nil {
			a.generalPurposeClient, err = NewGeneralPurposeClient(ctx, a.dbObject, a.database)
		} else if a.Xdsn != "" {
			a.generalPurposeClient, err = NewGeneralPurposeClientWithXDSN(ctx, a.Xdsn, a.database)
		} else if a.DSN != "" {
			a.generalPurposeClient, err = NewGeneralPurposeClientWithDSN(ctx, a.DSN, a.database)
		}
		a.initErr = err
	})
	return a.initErr
}

// initWriteClient initializes the write client once
func (a *SmartDatabaseAdapter) initWriteClient(ctx context.Context) error {
	a.onceWriteClient.Do(func() {
		var err error
		if a.dbObject != nil {
			a.writeClient, err = NewWriterClient(ctx, a.dbObject, a.database)
		} else if a.Xdsn != "" {
			a.writeClient, err = NewWriterClientWithXDSN(ctx, a.Xdsn, a.database)
		} else if a.DSN != "" {
			a.writeClient, err = NewWriterClientWithDSN(ctx, a.DSN, a.database)
		}
		a.initErr = err
	})
	return a.initErr
}

// Ping Delegate methods to the appropriate client
func (a *SmartDatabaseAdapter) Ping(ctx context.Context) error {
	if err := a.initWriteClient(ctx); err != nil {
		return err
	}
	return a.writeClient.Ping(ctx)
}

func (a *SmartDatabaseAdapter) Do(ctx context.Context, query ch.Query) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.initWriteClient(ctx); err != nil {
		return err
	}
	err := a.writeClient.Do(ctx, query)
	if err != nil {
		a.writeClient.Close()
		a.writeClient = nil
		a.onceWriteClient = sync.Once{}
	}
	return err
}

func (a *SmartDatabaseAdapter) Exec(ctx context.Context, query string, args ...any) error {
	if err := a.initGeneralClient(ctx); err != nil {
		return err
	}
	return a.generalPurposeClient.Exec(ctx, query, args...)
}

func (a *SmartDatabaseAdapter) Scan(ctx context.Context, req string, args []any, dest ...interface{}) error {
	if err := a.initGeneralClient(ctx); err != nil {
		return err
	}

	return a.generalPurposeClient.Scan(ctx, req, args, dest...)
}

// GetVersion Implement the GetVersion method in the adapter
func (a *SmartDatabaseAdapter) GetVersion(ctx context.Context, k uint64) (uint64, error) {
	if err := a.initGeneralClient(ctx); err != nil {
		return 0, err
	}

	return a.generalPurposeClient.GetVersion(ctx, k)
}

// TableExists Implement the TableExists method in the adapter
func (a *SmartDatabaseAdapter) TableExists(ctx context.Context, name string) (bool, error) {
	if err := a.initGeneralClient(ctx); err != nil {
		return false, err
	}
	return a.generalPurposeClient.TableExists(ctx, name)
}

// DropIfEmpty Implement the DropIfEmpty method in the adapter
func (a *SmartDatabaseAdapter) DropIfEmpty(ctx context.Context, name string) error {
	if err := a.initGeneralClient(ctx); err != nil {
		return err
	}

	return a.generalPurposeClient.DropIfEmpty(ctx, name)

}

// GetDBExec Implement the GetDBExec method in the adapter
func (a *SmartDatabaseAdapter) GetDBExec(env map[string]string) func(ctx context.Context, query string, args ...[]interface{}) error {
	if err := a.initGeneralClient(context.Background()); err != nil {
		return nil
	}
	return a.generalPurposeClient.GetDBExec(env)
}

// GetFirst Implement the GetFirst method in the adapter
func (a *SmartDatabaseAdapter) GetFirst(req string, first ...interface{}) error {
	if err := a.initGeneralClient(context.Background()); err != nil {
		return err
	}

	return a.generalPurposeClient.GetFirst(req, first...)
}

// GetList Implement the GetList method in the adapter
func (a *SmartDatabaseAdapter) GetList(req string) ([]string, error) {
	if err := a.initGeneralClient(context.Background()); err != nil {
		return nil, err
	}
	return a.generalPurposeClient.GetList(req)

}

// Close Implement the Close method in the adapter
func (a *SmartDatabaseAdapter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	var errs []error
	if a.generalPurposeClient != nil {
		errs = append(errs, a.generalPurposeClient.Close())
		a.onceGeneralClient = sync.Once{}
		a.generalPurposeClient = nil
	}
	if a.writeClient != nil {
		errs = append(errs, a.writeClient.Close())
		a.onceWriteClient = sync.Once{}
		a.writeClient = nil
	}
	var strErrs []string
	for _, err := range errs {
		if err != nil {
			strErrs = append(strErrs, err.Error())
		}
	}
	if len(strErrs) > 0 {
		return errors.New(strings.Join(strErrs, "; "))
	}
	return nil
}

// GetSetting Implement the GetSetting method in the adapter
func (a *SmartDatabaseAdapter) GetSetting(ctx context.Context, tp string, name string) (string, error) {
	if err := a.initGeneralClient(context.Background()); err != nil {
		return "", err
	}

	return a.generalPurposeClient.GetSetting(ctx, tp, name)
}

// PutSetting Implement the PutSetting method in the adapter
func (a *SmartDatabaseAdapter) PutSetting(ctx context.Context, tp string, name string, value string) error {
	if err := a.initGeneralClient(ctx); err != nil {
		return err
	}
	return a.generalPurposeClient.PutSetting(ctx, tp, name, value)
}

func (a *SmartDatabaseAdapter) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	if err := a.initGeneralClient(ctx); err != nil {
		return nil, err
	}
	return a.generalPurposeClient.Query(ctx, query, args...)
}

func (a *SmartDatabaseAdapter) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
	if err := a.initGeneralClient(ctx); err != nil {
		return nil
	}
	return a.generalPurposeClient.QueryRow(ctx, query, args...)
}
