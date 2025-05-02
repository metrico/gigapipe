package clickhouse_planner

import "github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"

func (p *planner) planFingerprints() (shared.SQLRequestPlanner, error) {
	var (
		labelNames []string
		ops        []string
		values     []string
	)
	for _, label := range p.script.StrSelector.StrSelCmds {
		labelNames = append(labelNames, label.Label.Name)
		ops = append(ops, label.Op)
		val, err := label.Val.Unquote()
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	res := NewStreamSelectPlanner(labelNames, ops, values, p.offsetModifier)
	return res, nil
}
