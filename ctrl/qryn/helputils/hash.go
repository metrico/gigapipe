package helputils

// Javascript port
func FingerprintLabelsDJBHashPrometheus(data []byte) uint32 {

	if data == nil {
		return 0
	}

	var hash int32 = 5381

	for i := len(data) - 1; i > -1; i-- {
		hash = (hash * 33) ^ int32(uint16(data[i]))
	}
	return uint32(hash)
}
