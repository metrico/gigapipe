package clickhouse_transpiler

import (
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

// listedIntrinsics are the intrinsics every listed span or trace carries in
// search results, so select() reads nothing for them.
var listedIntrinsics = map[string]bool{
	"duration": true, "span:duration": true, "span:id": true,
	"traceDuration": true, "trace:duration": true, "trace:id": true,
	"rootName": true, "trace:rootName": true, "rootServiceName": true, "trace:rootService": true,
}

// spanValueKeys lists the keys search reads per listed span, each once: the
// select() keys, then the spanset filters' keys.
func spanValueKeys(q *traceql_ir.Query) ([]string, error) {
	var keys []string
	seen := map[string]bool{}
	for _, f := range append(q.SelectedFields(), q.FilteredFields()...) {
		if listedIntrinsics[f.Name] {
			continue
		}
		key := f.Name
		if !traceql_ir.IsNestedSet(key) {
			ref, err := fieldRefOf(f.Name)
			if err != nil {
				return nil, err
			}
			key = ref.key
		}
		if !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	return keys, nil
}

// splitSelected separates the nested-set keys from the attribute keys.
func splitSelected(keys []string) (attrKeys, nestedSet []string) {
	for _, key := range keys {
		if traceql_ir.IsNestedSet(key) {
			nestedSet = append(nestedSet, key)
		} else {
			attrKeys = append(attrKeys, key)
		}
	}
	return attrKeys, nestedSet
}
