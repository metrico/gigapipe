package service

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/reader/utils/logger"
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

// MetadataResult represents metadata for a single metric
type MetadataResult struct {
	Type string `json:"type"`
	Help string `json:"help"`
	Unit string `json:"unit"`
}

// GetMetadata retrieves metadata for metrics
// metric: optional filter by metric name
// limit: optional limit on number of results
func (m *MetadataService) GetMetadata(ctx context.Context, metricFilter string, limit int) (map[string][]MetadataResult, error) {
	conn, err := m.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}

	table := "metrics_meta"
	if conn.Config.ClusterName != "" {
		table += "_dist"
	}

	// Build query to get metadata
	// Simple query without JOIN - we store metric_name directly
	metricFilterClause := ""
	args := []interface{}{}
	if metricFilter != "" {
		metricFilterClause = " AND metric_name = $1"
		args = append(args, metricFilter)
	}

	query := fmt.Sprintf(`
		SELECT 
			metric_name,
			argMax(value, timestamp_ms) as metadata_json
		FROM %s
		WHERE 1=1 %s
		GROUP BY metric_name
	`, table, metricFilterClause)

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := conn.Session.QueryCtx(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query metadata: %w", err)
	}
	defer rows.Close()

	result := make(map[string][]MetadataResult)

	for rows.Next() {
		var metricName string
		var metadataJSON string

		err := rows.Scan(&metricName, &metadataJSON)
		if err != nil {
			logger.Error("Failed to scan metadata row:", err)
			continue
		}

		// Parse JSON metadata
		md, err := metadata.FromJSON(metadataJSON)
		if err != nil {
			logger.Error("Failed to parse metadata JSON:", err)
			continue
		}

		// Add to result
		if _, exists := result[metricName]; !exists {
			result[metricName] = []MetadataResult{}
		}

		result[metricName] = append(result[metricName], MetadataResult{
			Type: md.Type,
			Help: md.Help,
			Unit: md.Unit,
		})
	}

	return result, nil
}

// GetMetadataByMetricName retrieves metadata for a specific metric name
func (m *MetadataService) GetMetadataByMetricName(ctx context.Context, metricName string) (*metadata.MetricMetadata, error) {
	conn, err := m.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}

	table := "metrics_meta"
	if conn.Config.ClusterName != "" {
		table += "_dist"
	}

	query := fmt.Sprintf(`
		SELECT argMax(value, timestamp_ms) as metadata_json
		FROM %s
		WHERE metric_name = $1
		GROUP BY metric_name
	`, table)

	rows, err := conn.Session.QueryCtx(ctx, query, metricName)
	if err != nil {
		return nil, fmt.Errorf("failed to query metadata: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil // No metadata found
	}

	var metadataJSON string
	err = rows.Scan(&metadataJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to scan metadata: %w", err)
	}

	return metadata.FromJSON(metadataJSON)
}

// GetMetadataByMetricNameList retrieves metadata for a metric by name (returns list)
// Simplified version - directly queries metrics_meta table by metric_name
func (m *MetadataService) GetMetadataByMetricNameList(ctx context.Context, metricName string) ([]MetadataResult, error) {
	conn, err := m.Session.GetDB(ctx)
	if err != nil {
		return nil, err
	}

	table := "metrics_meta"
	if conn.Config.ClusterName != "" {
		table += "_dist"
	}

	query := fmt.Sprintf(`
		SELECT argMax(value, timestamp_ms) as metadata_json
		FROM %s
		WHERE metric_name = $1
		GROUP BY metric_name
	`, table)

	rows, err := conn.Session.QueryCtx(ctx, query, metricName)
	if err != nil {
		return nil, fmt.Errorf("failed to query metadata: %w", err)
	}
	defer rows.Close()

	var results []MetadataResult
	for rows.Next() {
		var metadataJSON string

		err := rows.Scan(&metadataJSON)
		if err != nil {
			logger.Error("Failed to scan metadata row:", err)
			continue
		}

		md, err := metadata.FromJSON(metadataJSON)
		if err != nil {
			logger.Error("Failed to parse metadata JSON:", err)
			continue
		}

		results = append(results, MetadataResult{
			Type: md.Type,
			Help: md.Help,
			Unit: md.Unit,
		})
	}

	return results, nil
}

// ParseLimit parses limit parameter from string
func ParseLimit(limitStr string) int {
	if limitStr == "" {
		return 0 // No limit
	}
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 0 {
		return 0
	}
	return limit
}

