package metadata

import (
	"encoding/json"
	"fmt"
)

// Entry represents metadata for a Prometheus metric
type Entry struct {
	Type string `json:"type"` // counter, gauge, histogram, summary, untyped
	Help string `json:"help"` // Description of the metric
	Unit string `json:"unit"` // Unit of measurement
}

// ExtractMetadataFromLabels extracts metadata from special labels:
// "__metric_type__", "__metric_help__", or "__metric_unit__"
//
// Returns a zero-value Entry if no metadata labels are found
func ExtractMetadataFromLabels(labels [][]string) Entry {
	metadata := Entry{}

	for _, label := range labels {
		name := label[0]
		value := label[1]

		switch name {
		case "__metric_type__":
			metadata.Type = value
		case "__metric_help__":
			metadata.Help = value
		case "__metric_unit__":
			metadata.Unit = value
		}
	}

	return metadata
}

// IsZero returns true if no metadata fields are set
func (m Entry) IsZero() bool {
	return m.Type == "" && m.Help == "" && m.Unit == ""
}

// IsMetadataLabel returns true if the label name is a metadata label:
// "__metric_type__", "__metric_help__", or "__metric_unit__"
func IsMetadataLabel(name string) bool {
	return name == "__metric_type__" || name == "__metric_help__" || name == "__metric_unit__"
}

// ToJSON converts metadata to JSON string for storage
// Returns empty string if metadata is zero-value
func (m Entry) ToJSON() (string, error) {
	if m.IsZero() {
		return "", nil
	}
	data, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("failed to marshal metadata: %w", err)
	}
	return string(data), nil
}

// FromJSON parses metadata from JSON string
func FromJSON(jsonStr string) (Entry, error) {
	var metadata Entry
	err := json.Unmarshal([]byte(jsonStr), &metadata)
	if err != nil {
		return Entry{}, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}
	return metadata, nil
}
