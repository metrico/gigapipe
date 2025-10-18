package clickhouse_planner

import "github.com/metrico/qryn/reader/logql/logql_transpiler/shared"

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
	res := &StreamSelectPlanner{
		NoStreamSelect: p.noStreamSelect,
		LabelNames:     labelNames,
		Ops:            ops,
		Values:         values,
		Offset:         p.offsetModifier,
	}
	return res, nil
}
