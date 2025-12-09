package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/reader/utils/logger"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
	"github.com/metrico/qryn/v4/reader/utils/tables"
	"github.com/metrico/qryn/v4/writer/utils/metadata"
)

type MetadataService struct {
	model.ServiceData
}

func NewMetadataService(sd *model.ServiceData) *MetadataService {
	return &MetadataService{
		ServiceData: *sd,
	}
}

func (m *MetadataService) Metadata(ctx context.Context, metricFilter string, limit int, limitPerMetric int) (chan string, error) {
	conn, err := m.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}

	tableName := tables.GetTableName("time_series")
	if conn.Config.ClusterName != "" {
		tableName = tables.GetTableName("time_series_dist")
	}

	// Note: Extract metric name from labels JSON since name column is not populated
	// labels is stored as JSON like: {"__name__":"http_requests_total","job":"api"}
	// When limitPerMetric > 0, we return multiple metadata entries per metric using LIMIT BY clause
	// We use updated_at_ns for ordering and limiting, but don't need to SELECT it
	sel := sql.NewSelect().
		Select(
			sql.NewRawObject("JSONExtractString(labels, '__name__') as metric_name"),
			sql.NewRawObject("metadata as metadata_json"),
		).
		From(sql.NewRawObject(tableName)).
		AndWhere(sql.Neq(sql.NewRawObject("metadata"), sql.NewStringVal(""))).
		OrderBy(sql.NewRawObject("metric_name"), sql.NewRawObject("updated_at_ns DESC"))

	if metricFilter != "" {
		sel.AndWhere(sql.Eq(sql.NewRawObject("JSONExtractString(labels, '__name__')"), sql.NewStringVal(metricFilter)))
	}

	// LIMIT BY selects top N entries per metric
	if limitPerMetric > 0 {
		sel.Limit(sql.NewLimitBy(sql.NewIntVal(int64(limitPerMetric)), sql.NewRawObject("metric_name")))
	} else {
		// Default to 1 for missing limit_per_metric query param
		sel.Limit(sql.NewLimitBy(sql.NewIntVal(1), sql.NewRawObject("metric_name")))
	}

	if limit > 0 {
		sel.AddLimit(sql.NewIntVal(int64(limit)))
	}

	query, err := sel.String(&sql.Ctx{
		Params: map[string]sql.SQLObject{},
		Result: map[string]sql.SQLObject{},
	})
	if err != nil {
		return nil, err
	}

	rows, err := conn.Session.QueryCtx(ctx, query)
	if err != nil {
		return nil, err
	}

	// Stream JSON response in chunks to avoid buffering large results in memory.
	// Example ClickHouse result rows (2 columns - updated_at_ns used for ORDER BY/LIMIT BY but not selected):
	//   metric_name              | metadata_json
	//   "http_requests_total"    | {"type":"counter","help":"Total requests","unit":"requests"}
	//   "request_latency_seconds"| {"type":"histogram","help":"Latency v2","unit":"s"}
	//   "request_latency_seconds"| {"type":"histogram","help":"Latency distribution","unit":"seconds"}
	//
	// Output format with limit_per_metric=2:
	// {
	//   "status": "success",
	//   "data": {
	//     "http_requests_total": [{"type":"counter","help":"Total requests","unit":"requests"}],
	//     "request_latency_seconds": [
	//       {"type":"histogram","help":"Latency v2","unit":"s"},
	//       {"type":"histogram","help":"Latency distribution","unit":"seconds"}
	//     ]
	//   }
	// }
	res := make(chan string)
	go func() {
		defer close(res)
		defer rows.Close()

		// write initial opening braces
		res <- `{"status":"success","data":{`

		metricIndex := 0
		currentMetric := ""
		metricEntries := make([]metadata.Entry, 0)

		// write accumulated metric entries
		flushMetric := func() {
			if currentMetric == "" {
				return
			}

			// write commas on subsequent metrics
			if metricIndex > 0 {
				res <- ","
			}

			// write metric name and opening bracket
			encodedName, _ := json.Marshal(currentMetric)
			res <- string(encodedName) + ":["

			for i, entry := range metricEntries {
				// write commas on subsequent entries
				if i > 0 {
					res <- ","
				}
				// write entry JSON
				entryJSON, _ := json.Marshal(entry)
				res <- string(entryJSON)
			}
			// write closing bracket after entries
			res <- "]"
			metricIndex++
		}

		for rows.Next() {
			var metricName string
			var metadataJSON string

			err := rows.Scan(&metricName, &metadataJSON)
			if err != nil {
				logger.Error(err)
				break
			}

			entry, err := metadata.FromJSON(metadataJSON)
			if err != nil {
				logger.Error(err)
				continue
			}

			// new metric - flush and reset
			if currentMetric != metricName {
				flushMetric()
				currentMetric = metricName
				metricEntries = make([]metadata.Entry, 0)
			}

			metricEntries = append(metricEntries, entry)
		}

		flushMetric()

		// write final closing braces
		res <- "}}"
	}()

	return res, nil
}
