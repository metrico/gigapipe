package logql_transpiler

import (
	"errors"
	"io"
	"math"
	"sort"
	"strconv"

	log_parser "github.com/metrico/qryn/v4/reader/logql/logql_parser"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/internal/planner"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
)

// BinaryExprProcessor merges two independently-run RequestProcessorChains in memory,
// applying a binary arithmetic operator to matching (fingerprint, timestamp_ns) pairs.
// Both chains are started from nil (i.e. they source their own data from the DB).
type BinaryExprProcessor struct {
	Left        shared.RequestProcessorChain
	Right       shared.RequestProcessorChain // nil when IsScalar
	Op          string
	RightScalar float64
	IsScalar    bool
}

func (b *BinaryExprProcessor) IsMatrix() bool { return true }

func (b *BinaryExprProcessor) Process(ctx *shared.PlannerContext, _ chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	leftCh, err := b.Left[0].Process(ctx, nil)
	if err != nil {
		return nil, err
	}
	leftMap, err := drainMatrix(leftCh)
	if err != nil {
		return nil, err
	}

	out := make(chan []shared.LogEntry)

	if b.IsScalar {
		go func() {
			defer close(out)
			defer func() { shared.TamePanic(out) }()
			emitScalar(leftMap, b.Op, b.RightScalar, out)
		}()
		return out, nil
	}

	rightCh, err := b.Right[0].Process(ctx, nil)
	if err != nil {
		return nil, err
	}
	rightMap, err := drainMatrix(rightCh)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(out)
		defer func() { shared.TamePanic(out) }()
		emitBinary(leftMap, rightMap, b.Op, out)
	}()
	return out, nil
}

// sampleKey is the merge key for matrix entries.
type sampleKey struct {
	Fingerprint uint64
	TimestampNS int64
}

// drainMatrix consumes a matrix channel and collects all entries into a map.
// Handles both the io.EOF sentinel entry and plain channel close.
func drainMatrix(ch chan []shared.LogEntry) (map[sampleKey]shared.LogEntry, error) {
	m := make(map[sampleKey]shared.LogEntry)
	for batch := range ch {
		for _, e := range batch {
			if errors.Is(e.Err, io.EOF) {
				return m, nil
			}
			if e.Err != nil {
				return nil, e.Err
			}
			m[sampleKey{e.Fingerprint, e.TimestampNS}] = e
		}
	}
	return m, nil
}

func emitScalar(left map[sampleKey]shared.LogEntry, op string, scalar float64, out chan []shared.LogEntry) {
	keys := sortedSampleKeys(left)
	batch := make([]shared.LogEntry, 0, min(len(keys), 100))
	for _, k := range keys {
		e := left[k]
		e.Value = applyBinaryOp(e.Value, op, scalar)
		batch = append(batch, e)
		if len(batch) == 100 {
			out <- batch
			batch = make([]shared.LogEntry, 0, 100)
		}
	}
	out <- append(batch, shared.LogEntry{Err: io.EOF})
}

// emitBinary inner-joins left and right on (fingerprint, timestamp_ns).
// Entries present in only one side are dropped (Prometheus vector matching semantics).
func emitBinary(left, right map[sampleKey]shared.LogEntry, op string, out chan []shared.LogEntry) {
	var keys []sampleKey
	for k := range left {
		if _, ok := right[k]; ok {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Fingerprint != keys[j].Fingerprint {
			return keys[i].Fingerprint < keys[j].Fingerprint
		}
		return keys[i].TimestampNS < keys[j].TimestampNS
	})

	batch := make([]shared.LogEntry, 0, min(len(keys), 100))
	for _, k := range keys {
		e := left[k]
		e.Value = applyBinaryOp(e.Value, op, right[k].Value)
		batch = append(batch, e)
		if len(batch) == 100 {
			out <- batch
			batch = make([]shared.LogEntry, 0, 100)
		}
	}
	out <- append(batch, shared.LogEntry{Err: io.EOF})
}

func sortedSampleKeys(m map[sampleKey]shared.LogEntry) []sampleKey {
	keys := make([]sampleKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Fingerprint != keys[j].Fingerprint {
			return keys[i].Fingerprint < keys[j].Fingerprint
		}
		return keys[i].TimestampNS < keys[j].TimestampNS
	})
	return keys
}

func applyBinaryOp(left float64, op string, right float64) float64 {
	switch op {
	case "/":
		if right == 0 {
			return 0
		}
		return left / right
	case "*":
		return left * right
	case "+":
		return left + right
	case "-":
		return left - right
	case "%":
		if right == 0 {
			return 0
		}
		return math.Mod(left, right)
	}
	return 0
}

// planBinaryExprRAM plans a binary expression using in-process merging.
// Each operand is planned as an independent RequestProcessorChain (including its
// own ZeroEater + FixPeriod). The merged result has ZeroEater applied once more to
// eat zeros produced by the arithmetic itself (e.g. a - a = 0).
// FixPeriod is intentionally NOT re-applied: sub-chain timestamps are already at
// step intervals, and re-expanding them would duplicate entries.
func planBinaryExprRAM(script *log_parser.LogQLScript) (shared.RequestProcessorChain, error) {
	current, err := planAtomChain(script.Head)
	if err != nil {
		return nil, err
	}

	for _, binOp := range script.BinOps {
		var next shared.RequestProcessor

		if binOp.Right.Scalar != "" {
			scalar, err := strconv.ParseFloat(binOp.Right.Scalar, 64)
			if err != nil {
				return nil, err
			}
			next = &BinaryExprProcessor{
				Left:        current,
				Op:          binOp.Op,
				RightScalar: scalar,
				IsScalar:    true,
			}
		} else {
			right, err := planAtomChain(binOp.Right)
			if err != nil {
				return nil, err
			}
			next = &BinaryExprProcessor{
				Left:  current,
				Right: right,
				Op:    binOp.Op,
			}
		}

		current = shared.RequestProcessorChain{next}
	}

	var proc shared.RequestProcessor = &ZeroEaterPlanner{planner.GenericPlanner{Main: current[0]}}
	return shared.RequestProcessorChain{proc}, nil
}

// planAtomChain plans a single AtomExpr as a RequestProcessorChain.
func planAtomChain(atom log_parser.AtomExpr) (shared.RequestProcessorChain, error) {
	if atom.Paren != nil {
		return Plan(atom.Paren)
	}
	return Plan(&log_parser.LogQLScript{Head: atom})
}
