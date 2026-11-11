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

// selectedKeys lists the attribute keys the select() stages of q read for
// each listed span, each once in order.
func selectedKeys(q *traceql_ir.Query) ([]string, error) {
	var keys []string
	seen := map[string]bool{}
	for _, f := range q.SelectedFields() {
		if listedIntrinsics[f.Name] {
			continue
		}
		ref, err := fieldRefOf(f.Name)
		if err != nil {
			return nil, err
		}
		if !seen[ref.key] {
			seen[ref.key] = true
			keys = append(keys, ref.key)
		}
	}
	return keys, nil
}
