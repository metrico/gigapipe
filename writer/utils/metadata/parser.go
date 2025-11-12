package metadata

import (
	"encoding/json"
	"fmt"
	"strings"
)

// MetricMetadata represents metadata for a Prometheus metric
type MetricMetadata struct {
	Type string `json:"type"` // counter, gauge, histogram, summary, untyped
	Help string `json:"help"` // Description of the metric
	Unit string `json:"unit"` // Unit of measurement
}

// ParseMetadataFromHeader parses metadata from X-Scope-Meta header
// Format: "metric_name:type:help:unit" or JSON format
func ParseMetadataFromHeader(headerValue string) (map[string]*MetricMetadata, error) {
	if headerValue == "" {
		return nil, nil
	}

	// Try JSON format first
	if strings.TrimSpace(headerValue)[0] == '{' {
		return parseJSONMetadata(headerValue)
	}

	// Parse colon-separated format: "metric_name:type:help:unit"
	return parseColonSeparatedMetadata(headerValue)
}

// parseJSONMetadata parses JSON format metadata
// Format: {"metric_name": {"type": "counter", "help": "...", "unit": "..."}}
func parseJSONMetadata(jsonStr string) (map[string]*MetricMetadata, error) {
	var data map[string]*MetricMetadata
	err := json.Unmarshal([]byte(jsonStr), &data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON metadata: %w", err)
	}
	return data, nil
}

// parseColonSeparatedMetadata parses colon-separated format
// Format: "metric_name:type:help:unit"
// Multiple metrics can be separated by newlines or semicolons
func parseColonSeparatedMetadata(headerValue string) (map[string]*MetricMetadata, error) {
	result := make(map[string]*MetricMetadata)

	// Split by newlines or semicolons for multiple metrics
	lines := strings.Split(headerValue, "\n")
	for _, line := range lines {
		// Also support semicolon separation
		parts := strings.Split(line, ";")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}

			// Split by colon: metric_name:type:help:unit
			fields := strings.Split(part, ":")
			if len(fields) < 2 {
				continue // Skip invalid format
			}

			metricName := strings.TrimSpace(fields[0])
			if metricName == "" {
				continue
			}

			metadata := &MetricMetadata{}
			if len(fields) >= 2 {
				metadata.Type = strings.TrimSpace(fields[1])
			}
			if len(fields) >= 3 {
				metadata.Help = strings.TrimSpace(fields[2])
			}
			if len(fields) >= 4 {
				metadata.Unit = strings.TrimSpace(fields[3])
			}

			result[metricName] = metadata
		}
	}

	return result, nil
}

// ExtractMetadataFromLabels extracts metadata from special labels
// Looks for __metric_type__, __metric_help__, __metric_unit__ labels
func ExtractMetadataFromLabels(labels [][]string) (map[string]*MetricMetadata, map[string]bool) {
	metadataMap := make(map[string]*MetricMetadata)
	labelsToRemove := make(map[string]bool)

	var currentMetricName string
	var currentMetadata *MetricMetadata

	for _, label := range labels {
		name := label[0]
		value := label[1]

		// Find metric name (label with __name__)
		if name == "__name__" {
			// Save previous metric metadata if exists
			if currentMetricName != "" && currentMetadata != nil {
				metadataMap[currentMetricName] = currentMetadata
			}
			currentMetricName = value
			currentMetadata = &MetricMetadata{}
			continue
		}

		// Extract metadata from special labels
		switch name {
		case "__metric_type__":
			if currentMetadata == nil {
				currentMetadata = &MetricMetadata{}
			}
			currentMetadata.Type = value
			labelsToRemove[name] = true
		case "__metric_help__":
			if currentMetadata == nil {
				currentMetadata = &MetricMetadata{}
			}
			currentMetadata.Help = value
			labelsToRemove[name] = true
		case "__metric_unit__":
			if currentMetadata == nil {
				currentMetadata = &MetricMetadata{}
			}
			currentMetadata.Unit = value
			labelsToRemove[name] = true
		}
	}

	// Save last metric metadata
	if currentMetricName != "" && currentMetadata != nil {
		metadataMap[currentMetricName] = currentMetadata
	}

	return metadataMap, labelsToRemove
}

// ToJSON converts metadata to JSON string for storage
func (m *MetricMetadata) ToJSON() (string, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("failed to marshal metadata: %w", err)
	}
	return string(data), nil
}

// FromJSON parses metadata from JSON string
func FromJSON(jsonStr string) (*MetricMetadata, error) {
	var metadata MetricMetadata
	err := json.Unmarshal([]byte(jsonStr), &metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}
	return &metadata, nil
}

