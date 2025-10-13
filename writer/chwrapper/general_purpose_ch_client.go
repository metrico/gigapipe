package chwrapper

import (
	"bytes"
	"context"
	"fmt"
	rand2 "math/rand"
	"strconv"
	"text/template"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/metrico/qryn/v4/writer/utils/helputils"
	"github.com/metrico/qryn/v4/writer/utils/logger"
)

type Client struct {
	c driver.Conn
}

var _ IChClient = &Client{}

func (c *Client) Scan(ctx context.Context, req string, args []any, dest ...interface{}) error {
	rows, err := c.c.Query(ctx, req, args...)
	if err != nil {
		return err
	}
	defer func(rows driver.Rows) {
		err := rows.Close()
		if err != nil {
			logger.Error(err)
		}
	}(rows)
	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) DropIfEmpty(ctx context.Context, name string) error {
	exists, err := c.TableExists(ctx, name)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	empty, err := c.tableEmpty(ctx, name)
	if err != nil {
		return err
	}
	if !empty {
		return nil
	}
	err = c.c.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
	return err
}

func (c *Client) GetVersion(ctx context.Context, k uint64) (uint64, error) {
	rows, err := c.c.Query(ctx, "SELECT max(ver) as ver FROM ver WHERE k = $1 FORMAT JSON", k)
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

func (c *Client) TableExists(ctx context.Context, name string) (bool, error) {
	rows, err := c.c.Query(ctx, "SHOW TABLES")
	if err != nil {
		return false, err
	}
	defer func(rows driver.Rows) {
		err := rows.Close()
		if err != nil {
			logger.Error(err)
		}
	}(rows)
	for rows.Next() {
		var _name string
		err = rows.Scan(&_name)
		if err != nil {
			return false, err
		}
		if _name == name {
			return true, nil
		}
	}
	return false, nil
}

func (c *Client) tableEmpty(ctx context.Context, name string) (bool, error) {
	rows, err := c.c.Query(ctx, fmt.Sprintf("SELECT count(1) FROM %s", name))
	if err != nil {
		return false, err
	}
	defer func(rows driver.Rows) {
		err := rows.Close()
		if err != nil {
			logger.Error(err)
		}
	}(rows)
	rows.Next()
	var count uint64
	err = rows.Scan(&count)
	return count == 0, err
}

func (c *Client) Exec(ctx context.Context, query string, args ...any) error {

	logger.Info("query Info", query)
	return c.c.Exec(ctx, query, args)
}

func (c *Client) GetDBExec(env map[string]string) func(ctx context.Context, query string, args ...[]interface{}) error {
	rand := rand2.New(rand2.NewSource(time.Now().UnixNano()))
	return func(ctx context.Context, query string, args ...[]interface{}) error {
		name := fmt.Sprintf("tpl_%d", rand.Uint64())
		tpl, err := template.New(name).Parse(query)
		if err != nil {
			logger.Error(query)
			return err
		}
		buf := bytes.NewBuffer(nil)
		err = tpl.Execute(buf, env)
		if err != nil {
			logger.Error(query)
			return err
		}
		req := buf.String()
		logger.Info(req)
		err = c.c.Exec(ctx, req)
		if err != nil {
			logger.Error(req)
			return err
		}
		return nil
	}
}

func (c *Client) GetFirst(req string, first ...interface{}) error {
	res, err := c.c.Query(context.Background(), req)
	if err != nil {
		return err
	}
	defer res.Close()
	res.Next()
	err = res.Scan(first...)
	return err
}

func (c *Client) GetList(req string) ([]string, error) {
	res, err := c.c.Query(context.Background(), req)
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

func (c *Client) Close() error {
	return c.c.Close()
}

func (c *Client) GetSetting(ctx context.Context, tp string, name string) (string, error) {
	fp := helputils.FingerprintLabelsDJBHashPrometheus([]byte(
		fmt.Sprintf(`{"type":%s, "name":%s`, strconv.Quote(tp), strconv.Quote(name)),
	))
	rows, err := c.c.Query(ctx, `SELECT argMax(value, inserted_at) as _value FROM settings WHERE fingerprint = $1 
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

func (c *Client) PutSetting(ctx context.Context, tp string, name string, value string) error {
	_name := fmt.Sprintf(`{"type":%s, "name":%s`, strconv.Quote(tp), strconv.Quote(name))
	fp := helputils.FingerprintLabelsDJBHashPrometheus([]byte(_name))
	err := c.c.Exec(ctx, `INSERT INTO settings (fingerprint, type, name, value, inserted_at)
VALUES ($1, $2, $3, $4, NOW())`, fp, tp, name, value)
	return err
}

func (c *Client) Ping(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (c *Client) Do(ctx context.Context, query ch.Query) error {
	//TODO implement me
	panic("implement me")
}

func (c *Client) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	// Call the ClickHouse Query method on the connection object with the provided query and arguments
	rows, err := c.c.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (c *Client) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
	// Call the QueryRow method from the underlying ClickHouse connection
	return c.c.QueryRow(ctx, query, args...)
}
