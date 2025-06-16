package ch_wrapper

import (
	"context"
	"fmt"
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/metrico/qryn/writer/utils/heputils"
	"github.com/metrico/qryn/writer/utils/logger"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// HttpChClient implements the IChClient interface for HTTP connections
type HttpChClient struct {
	conn clickhouse.Conn
}

func NewHttpChClientFactory(dsn string) IChClientFactory {
	return func() (IChClient, error) {
		options, err := clickhouse.ParseDSN(dsn)
		if err != nil {
			options = &clickhouse.Options{
				Addr: []string{dsn},
				Auth: clickhouse.Auth{
					Database: "default",
					Username: "default",
					Password: "",
				},
				Protocol: clickhouse.HTTP,
				Settings: clickhouse.Settings{
					"max_execution_time": 60,
				},
				DialTimeout:      time.Second * 30,
				MaxOpenConns:     10,
				MaxIdleConns:     5,
				ConnMaxLifetime:  time.Hour,
				ConnOpenStrategy: clickhouse.ConnOpenInOrder,
			}
		} else {
			options.Protocol = clickhouse.HTTP
		}

		conn, err := clickhouse.Open(options)
		if err != nil {
			return nil, fmt.Errorf("failed to open HTTP connection to ClickHouse: %w", err)
		}

		return &HttpChClient{
			conn: conn,
		}, nil
	}
}

func (c *HttpChClient) Ping(ctx context.Context) error {
	if c.conn == nil {
		return fmt.Errorf("connection is nil")
	}
	return c.conn.Ping(ctx)
}

func (c *HttpChClient) Do(ctx context.Context, query ch.Query) error {
	if c.conn == nil {
		return fmt.Errorf("connection is nil")
	}

	sqlQuery := strings.TrimSpace(query.Body)
	if sqlQuery == "" {
		return fmt.Errorf("query body is empty")
	}

	// Handle INSERT with data
	if len(query.Input) > 0 {
		return c.executeInsert(ctx, sqlQuery, query.Input)
	}
	return fmt.Errorf("input Data is empty")
}
func (c *HttpChClient) executeInsert(ctx context.Context, sql string, input proto.Input) error {

	if len(input) == 0 {
		return nil
	}

	// Get row count from first column
	rowCount := c.getRowCount(input[0].Data)
	if rowCount == 0 {
		return nil
	}

	//// Direct call to c.conn.PrepareBatch
	//var batch interface{}
	//var err error

	batch, err := c.conn.PrepareBatch(ctx, sql)

	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	// Insert rows
	for row := 0; row < rowCount; row++ {
		values := make([]interface{}, len(input))
		for col, column := range input {
			values[col] = c.getValue(column.Data, row)
		}

		if err := batch.Append(values...); err != nil {
			return fmt.Errorf("failed to append row %d: %w", row, err)
		}
	}

	// Send the batch
	return batch.Send()
}

func (c *HttpChClient) getValue(data interface{}, row int) interface{} {
	if data == nil {
		return nil
	}
	if v, ok := data.(interface{ Row(int) interface{} }); ok {
		return v.Row(row)
	}
	if v, ok := data.(interface{ Get(int) interface{} }); ok {
		return v.Get(row)
	}
	if v, ok := data.(interface{ Value(int) interface{} }); ok {
		return v.Value(row)
	}

	// Use reflection
	rv := reflect.ValueOf(data)
	if rv.Kind() == reflect.Ptr && !rv.IsNil() {
		rv = rv.Elem()
	}

	// Direct indexing for slices/arrays
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		if row < rv.Len() {
			return rv.Index(row).Interface()
		}
		return nil
	}

	// Try reflection methods
	for _, method := range []string{"Row", "Get", "Value", "At"} {
		if m := rv.MethodByName(method); m.IsValid() {
			if m.Type().NumIn() == 1 && m.Type().In(0).Kind() == reflect.Int {
				result := m.Call([]reflect.Value{reflect.ValueOf(row)})
				if len(result) > 0 {
					return result[0].Interface()
				}
			}
		}
	}

	return nil
}

func (c *HttpChClient) getRowCount(data interface{}) int {
	if data == nil {
		return 0
	}

	// Try common interfaces
	if v, ok := data.(interface{ Rows() int }); ok {
		return v.Rows()
	}
	if v, ok := data.(interface{ Len() int }); ok {
		return v.Len()
	}

	// Use reflection
	rv := reflect.ValueOf(data)
	if rv.Kind() == reflect.Ptr && !rv.IsNil() {
		rv = rv.Elem()
	}

	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		return rv.Len()
	}

	for _, method := range []string{"Rows", "Len", "Size", "Count"} {
		if m := rv.MethodByName(method); m.IsValid() {
			if m.Type().NumIn() == 0 && m.Type().NumOut() == 1 {
				result := m.Call(nil)
				if len(result) > 0 && result[0].Kind() == reflect.Int {
					return int(result[0].Int())
				}
			}
		}
	}

	return 0
}

func (c *HttpChClient) Exec(ctx context.Context, query string, args ...any) error {
	if c.conn == nil {
		return fmt.Errorf("connection is nil")
	}
	return c.conn.Exec(ctx, query, args...)
}

// ServerVersion returns the ClickHouse server version
func (c *HttpChClient) ServerVersion() (*driver.ServerVersion, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("connection is nil")
	}
	return c.conn.ServerVersion()
}

func (c *HttpChClient) GetFirst(req string, first ...interface{}) error {
	res, err := c.Query(context.Background(), req)
	if err != nil {
		return err
	}
	defer res.Close()
	res.Next()
	err = res.Scan(first...)
	return err
}

func (c *HttpChClient) Scan(ctx context.Context, req string, args []any, dest ...interface{}) error {
	if c.conn == nil {
		return fmt.Errorf("connection is nil")
	}
	row := c.conn.QueryRow(ctx, req, args...)
	return row.Scan(dest...)
}

func (c *HttpChClient) DropIfEmpty(ctx context.Context, name string) error {
	if c.conn == nil {
		return fmt.Errorf("connection is nil")
	}

	// Check if table is empty
	countQuery := fmt.Sprintf("SELECT count(*) FROM %s", name)
	row := c.conn.QueryRow(ctx, countQuery)

	var count int64
	if err := row.Scan(&count); err != nil {
		return fmt.Errorf("failed to check table count: %w", err)
	}

	// Drop if empty
	if count == 0 {
		dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", name)
		return c.conn.Exec(ctx, dropQuery)
	}

	return nil
}

func (c *HttpChClient) TableExists(ctx context.Context, name string) (bool, error) {
	if c.conn == nil {
		return false, fmt.Errorf("connection is nil")
	}

	query := "SELECT 1 FROM system.tables WHERE name = ? LIMIT 1"
	row := c.conn.QueryRow(ctx, query, name)

	var exists int
	err := row.Scan(&exists)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return false, nil
		}
		return false, err
	}
	return exists == 1, nil
}

func (c *HttpChClient) GetDBExec(env map[string]string) func(ctx context.Context, query string, args ...[]interface{}) error {
	return func(ctx context.Context, query string, args ...[]interface{}) error {
		if c.conn == nil {
			return fmt.Errorf("connection is nil")
		}
		// Convert [][]interface{} to []interface{} if needed
		var flatArgs []interface{}
		for _, argGroup := range args {
			for _, arg := range argGroup {
				flatArgs = append(flatArgs, arg)
			}
		}
		return c.conn.Exec(ctx, query, flatArgs...)
	}
}

func (c *HttpChClient) GetVersion(ctx context.Context, k uint64) (uint64, error) {
	rows, err := c.Query(ctx, "SELECT max(ver) as ver FROM ver WHERE k = $1 FORMAT JSON", k)
	if err != nil {
		return 0, err
	}
	var ver uint64 = 0
	for rows.Next() {
		err = rows.Scan(&ver)
		if err != nil {
			return 0, err
		}
	}
	return ver, nil
}

func (c *HttpChClient) GetSetting(ctx context.Context, tp string, name string) (string, error) {
	fp := heputils.FingerprintLabelsDJBHashPrometheus([]byte(
		fmt.Sprintf(`{"type":%s, "name":%s`, strconv.Quote(tp), strconv.Quote(name)),
	))
	rows, err := c.Query(ctx, `SELECT argMax(value, inserted_at) as _value FROM settings WHERE fingerprint = $1 
GROUP BY fingerprint HAVING argMax(name, inserted_at) != ''`, fp)
	if err != nil {
		return "", err
	}
	res := ""
	for rows.Next() {
		err = rows.Scan(&res)
		if err != nil {
			return "", err
		}
	}
	return res, nil
}

func (c *HttpChClient) PutSetting(ctx context.Context, tp string, name string, value string) error {
	_name := fmt.Sprintf(`{"type":%s, "name":%s`, strconv.Quote(tp), strconv.Quote(name))
	fp := heputils.FingerprintLabelsDJBHashPrometheus([]byte(_name))
	err := c.Exec(ctx, `INSERT INTO settings (fingerprint, type, name, value, inserted_at)
VALUES ($1, $2, $3, $4, NOW())`, fp, tp, name, value)
	return err
}

func (c *HttpChClient) GetList(req string) ([]string, error) {
	res, err := c.Query(context.Background(), req)
	if err != nil {
		logger.Error("GetList Error", err.Error())
		return nil, err
	}
	defer res.Close()
	arr := make([]string, 0)
	for res.Next() {
		var val string
		err = res.Scan(&val)
		if err != nil {
			logger.Error("GetList Error", err.Error())
			return nil, err
		}
		arr = append(arr, val)
	}
	return arr, nil
}

func (c *HttpChClient) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("connection is nil")
	}
	return c.conn.Query(ctx, query, args...)
}
func (c *HttpChClient) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
	if c.conn == nil {
		return &errorRow{err: fmt.Errorf("connection is nil")}
	}
	return c.conn.QueryRow(ctx, query, args...)
}

// Close closes the connection
func (c *HttpChClient) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// errorRow is a helper type for returning errors from QueryRow when connection is nil
type errorRow struct {
	err error
}

func (r *errorRow) Scan(dest ...interface{}) error {
	return r.err
}

func (r *errorRow) ScanStruct(dest interface{}) error {
	return r.err
}

func (r *errorRow) Err() error {
	return r.err
}

//func TestHttpConnection(factory IHttpChClientFactory) error {
//	client, err := factory()
//	if err != nil {
//		return fmt.Errorf("failed to create client: %w", err)
//	}
//	defer client.Close()
//
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//
//	if err := client.Ping(ctx); err != nil {
//		return fmt.Errorf("ping failed: %w", err)
//	}
//
//	return nil
//}
