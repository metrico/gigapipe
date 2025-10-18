package shared

import (
	"database/sql"
	"io"

	sql2 "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

type ClickhouseGetterPlanner struct {
	ClickhouseRequestPlanner SQLRequestPlanner
	Matrix                   bool
}

func (c *ClickhouseGetterPlanner) IsMatrix() bool {
	return c.Matrix
}

func (c *ClickhouseGetterPlanner) Process(ctx *PlannerContext,
	ch chan []LogEntry) (chan []LogEntry, error) {
	req, err := c.ClickhouseRequestPlanner.Process(ctx)
	if err != nil {
		return nil, err
	}
	var options []int
	if ctx.IsCluster {
		options = append(options, sql2.STRING_OPT_INLINE_WITH)
	}
	strReq, err := req.String(ctx.CHSqlCtx, options...)
	if err != nil {
		return nil, err
	}
	rows, err := ctx.CHDb.QueryCtx(ctx.Ctx, strReq)
	if err != nil {
		return nil, err
	}
	res := make(chan []LogEntry)
	if !c.Matrix {
		go c.Scan(ctx, rows, res)
	} else {
		go c.ScanMatrix(ctx, rows, res)
	}

	return res, nil
}

func (c *ClickhouseGetterPlanner) Scan(ctx *PlannerContext, rows *sql.Rows, res chan []LogEntry) {
	defer rows.Close()
	defer close(res)
	entries := make([]LogEntry, 100)
	i := 0

	for rows.Next() {
		select {
		case <-ctx.Ctx.Done():
			if len(entries) > 0 {
				res <- entries[:i]
			}
			return
		default:
		}
		var (
			labels map[string]string
		)
		err := rows.Scan(&entries[i].Fingerprint, &labels, &entries[i].Message, &entries[i].TimestampNS)
		if err != nil {
			entries[i].Err = err
			res <- entries[:i+1]
			return
		}
		entries[i].Labels = make(map[string]string)
		for k, v := range labels {
			entries[i].Labels[k] = v
		}
		i++
		if i >= 100 {
			res <- entries
			entries = make([]LogEntry, 100)
			i = 0
		}
	}
	entries[i].Err = io.EOF
	res <- entries[:i+1]
}

func (c *ClickhouseGetterPlanner) ScanMatrix(ctx *PlannerContext, rows *sql.Rows, res chan []LogEntry) {
	defer rows.Close()
	defer close(res)
	entries := make([]LogEntry, 100)
	i := 0

	for rows.Next() {
		select {
		case <-ctx.Ctx.Done():
			if len(entries) > 0 {
				res <- entries[:i]
			}
			return
		default:
		}
		var (
			labels map[string]string
		)
		err := rows.Scan(&entries[i].Fingerprint, &labels, &entries[i].Value,
			&entries[i].TimestampNS)
		if err != nil {
			entries[i].Err = err
			res <- entries[:i+1]
			return
		}
		entries[i].Labels = make(map[string]string)
		for k, v := range labels {
			entries[i].Labels[k] = v
		}
		i++
		if i >= 100 {
			res <- entries
			entries = make([]LogEntry, 100)
			i = 0
		}
	}
	entries[i].Err = io.EOF
	res <- entries[:i+1]
}
