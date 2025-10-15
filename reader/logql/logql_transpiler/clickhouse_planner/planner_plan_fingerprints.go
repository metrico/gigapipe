package clickhouse_planner

import "github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"

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
