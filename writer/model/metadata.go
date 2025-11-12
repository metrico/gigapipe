package model

// MetadataData represents metadata to be inserted into metrics_meta table
type MetadataData struct {
	MetricNames  []string
	MetadataJSON []string
	Size         int
}

func (m *MetadataData) GetSize() int64 {
	return int64(m.Size)
}

