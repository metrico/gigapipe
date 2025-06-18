package ch_wrapper

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/metrico/qryn/writer/utils/heputils"
	"github.com/metrico/qryn/writer/utils/logger"
	"reflect"
	"strconv"
	"strings"
)

// HttpChClient implements the IChClient interface for HTTP connections
type HttpChClient struct {
	db *sql.DB
}

func NewHttpChClientFactory(dsn string) IChClientFactory {
	return func() (IChClient, error) {
		options, err := clickhouse.ParseDSN(dsn)
		if err != nil {
			return nil, err
		}

		// Ensure we're using HTTP protocol
		options.Protocol = clickhouse.HTTP

		db := clickhouse.OpenDB(options)
		if err := db.Ping(); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to open HTTP connection to ClickHouse: %w", err)
		}

		return &HttpChClient{
			db: db,
		}, nil
	}
}

func (c *HttpChClient) Ping(ctx context.Context) error {
	if c.db == nil {
		return fmt.Errorf("connection is nil")
	}
	return c.db.PingContext(ctx)
}

func (c *HttpChClient) Do(ctx context.Context, query ch.Query) error {
	if c.db == nil {
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

	// Start transaction
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Prepare statement
	stmt, err := tx.PrepareContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Insert rows
	for row := 0; row < rowCount; row++ {
		values := make([]interface{}, len(input))
		for col, column := range input {
			values[col] = c.getValue(column.Data, row)
		}

		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("failed to execute row %d: %w", row, err)
		}
	}

	// Commit transaction
	return tx.Commit()
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
	if c.db == nil {
		return fmt.Errorf("connection is nil")
	}
	_, err := c.db.ExecContext(ctx, query, args...)
	return err
}

// ServerVersion returns the ClickHouse server version
// Note: This method is limited with SQL interface - we'll query the version
func (c *HttpChClient) ServerVersion() (*driver.ServerVersion, error) {
	if c.db == nil {
		return nil, fmt.Errorf("connection is nil")
	}

	var version string
	err := c.db.QueryRowContext(context.Background(), "SELECT version()").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get server version: %w", err)
	}

	// Parse version string (simplified)
	return &driver.ServerVersion{
		Name:        "ClickHouse",
		DisplayName: version,
	}, nil
}

func (c *HttpChClient) GetFirst(req string, first ...interface{}) error {
	if c.db == nil {
		return fmt.Errorf("connection is nil")
	}
	return c.db.QueryRowContext(context.Background(), req).Scan(first...)
}

func (c *HttpChClient) Scan(ctx context.Context, req string, args []any, dest ...interface{}) error {
	if c.db == nil {
		return fmt.Errorf("connection is nil")
	}
	return c.db.QueryRowContext(ctx, req, args...).Scan(dest...)
}

func (c *HttpChClient) DropIfEmpty(ctx context.Context, name string) error {
	if c.db == nil {
		return fmt.Errorf("connection is nil")
	}

	// Check if table is empty
	countQuery := fmt.Sprintf("SELECT count(*) FROM %s", name)
	var count int64
	if err := c.db.QueryRowContext(ctx, countQuery).Scan(&count); err != nil {
		return fmt.Errorf("failed to check table count: %w", err)
	}

	// Drop if empty
	if count == 0 {
		dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", name)
		_, err := c.db.ExecContext(ctx, dropQuery)
		return err
	}

	return nil
}

func (c *HttpChClient) TableExists(ctx context.Context, name string) (bool, error) {
	if c.db == nil {
		return false, fmt.Errorf("connection is nil")
	}

	query := "SELECT 1 FROM system.tables WHERE name = ? LIMIT 1"
	var exists int
	err := c.db.QueryRowContext(ctx, query, name).Scan(&exists)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return exists == 1, nil
}

func (c *HttpChClient) GetDBExec(env map[string]string) func(ctx context.Context, query string, args ...[]interface{}) error {
	return func(ctx context.Context, query string, args ...[]interface{}) error {
		if c.db == nil {
			return fmt.Errorf("connection is nil")
		}
		// Convert [][]interface{} to []interface{} if needed
		var flatArgs []interface{}
		for _, argGroup := range args {
			for _, arg := range argGroup {
				flatArgs = append(flatArgs, arg)
			}
		}
		_, err := c.db.ExecContext(ctx, query, flatArgs...)
		return err
	}
}

func (c *HttpChClient) GetVersion(ctx context.Context, k uint64) (uint64, error) {
	rows, err := c.Query(ctx, "SELECT max(ver) as ver FROM ver WHERE k = ? FORMAT JSON", k)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var ver uint64 = 0
	if rows.Next() {
		err = rows.Scan(&ver)
		if err != nil {
			return 0, err
		}
	}
	return ver, rows.Err()
}

func (c *HttpChClient) GetSetting(ctx context.Context, tp string, name string) (string, error) {
	fp := heputils.FingerprintLabelsDJBHashPrometheus([]byte(
		fmt.Sprintf(`{"type":%s, "name":%s`, strconv.Quote(tp), strconv.Quote(name)),
	))
	rows, err := c.Query(ctx, `SELECT argMax(value, inserted_at) as _value FROM settings WHERE fingerprint = ? 
GROUP BY fingerprint HAVING argMax(name, inserted_at) != ''`, fp)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	res := ""
	if rows.Next() {
		err = rows.Scan(&res)
		if err != nil {
			return "", err
		}
	}
	return res, rows.Err()
}

func (c *HttpChClient) PutSetting(ctx context.Context, tp string, name string, value string) error {
	_name := fmt.Sprintf(`{"type":%s, "name":%s`, strconv.Quote(tp), strconv.Quote(name))
	fp := heputils.FingerprintLabelsDJBHashPrometheus([]byte(_name))
	err := c.Exec(ctx, `INSERT INTO settings (fingerprint, type, name, value, inserted_at)
VALUES (?, ?, ?, ?, NOW())`, fp, tp, name, value)
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
	return arr, res.Err()
}

func (c *HttpChClient) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	if c.db == nil {
		return nil, fmt.Errorf("connection is nil")
	}

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return &sqlRowsWrapper{rows: rows}, nil
}

func (c *HttpChClient) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
	if c.db == nil {
		return &errorRow{err: fmt.Errorf("connection is nil")}
	}

	row := c.db.QueryRowContext(ctx, query, args...)
	return &sqlRowWrapper{row: row}
}

// Close closes the connection
func (c *HttpChClient) Close() error {
	if c.db == nil {
		return nil
	}
	return c.db.Close()
}

// sqlRowsWrapper wraps sql.Rows to implement driver.Rows interface
type sqlRowsWrapper struct {
	rows *sql.Rows
}

func (r *sqlRowsWrapper) Next() bool {
	return r.rows.Next()
}

func (r *sqlRowsWrapper) Scan(dest ...interface{}) error {
	return r.rows.Scan(dest...)
}

func (r *sqlRowsWrapper) ScanStruct(dest interface{}) error {
	// This is a simplified implementation - you might need to use reflection
	// or a third-party library for proper struct scanning
	return fmt.Errorf("ScanStruct not implemented for HTTP client")
}

func (r *sqlRowsWrapper) Close() error {
	return r.rows.Close()
}

func (r *sqlRowsWrapper) Err() error {
	return r.rows.Err()
}

func (r *sqlRowsWrapper) Columns() []string {
	columns, err := r.rows.Columns()
	if err != nil {
		return nil
	}
	return columns
}

func (r *sqlRowsWrapper) ColumnTypes() []driver.ColumnType {
	sqlColumnTypes, err := r.rows.ColumnTypes()
	if err != nil {
		return nil
	}

	columnTypes := make([]driver.ColumnType, len(sqlColumnTypes))
	for i, sqlCol := range sqlColumnTypes {
		columnTypes[i] = &columnTypeWrapper{sqlCol}
	}
	return columnTypes
}

func (r *sqlRowsWrapper) Totals(dest ...interface{}) error {
	// HTTP interface doesn't support totals
	return fmt.Errorf("totals not supported in HTTP interface")
}

// columnTypeWrapper wraps sql.ColumnType to implement driver.ColumnType interface
type columnTypeWrapper struct {
	sqlColumnType *sql.ColumnType
}

func (c *columnTypeWrapper) Name() string {
	return c.sqlColumnType.Name()
}

func (c *columnTypeWrapper) DatabaseTypeName() string {
	return c.sqlColumnType.DatabaseTypeName()
}

func (c *columnTypeWrapper) ScanType() reflect.Type {
	return c.sqlColumnType.ScanType()
}

func (c *columnTypeWrapper) Nullable() bool {
	nullable, ok := c.sqlColumnType.Nullable()
	return ok && nullable
}

func (c *columnTypeWrapper) Length() (length int64, ok bool) {
	return c.sqlColumnType.Length()
}

func (c *columnTypeWrapper) DecimalSize() (precision, scale int64, ok bool) {
	return c.sqlColumnType.DecimalSize()
}

// sqlRowWrapper wraps sql.Row to implement driver.Row interface
type sqlRowWrapper struct {
	row *sql.Row
}

func (r *sqlRowWrapper) Scan(dest ...interface{}) error {
	return r.row.Scan(dest...)
}

func (r *sqlRowWrapper) ScanStruct(dest interface{}) error {
	// This is a simplified implementation
	return fmt.Errorf("ScanStruct not implemented for HTTP client")
}

func (r *sqlRowWrapper) Err() error {
	// sql.Row doesn't have an Err() method, so we return nil
	return nil
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
