package service

import (
	"sort"

	"github.com/metrico/qryn/v4/reader/prof"
)

type Tree struct {
	Names       []string
	NamesMap    map[uint64]int
	Nodes       map[uint64][]*TreeNodeV2
	SampleTypes []string
	maxSelf     []int64
	NodesNum    int32
	Pprof       *prof.Profile
}

func NewTree() *Tree {
	return &Tree{
		Nodes:    make(map[uint64][]*TreeNodeV2),
		NamesMap: map[uint64]int{},
		Names:    []string{"total", "n/a"},
		maxSelf:  []int64{0},
	}
}

func (t *Tree) MaxSelf() []int64 {
	if len(t.maxSelf) == 0 {
		return []int64{0}
	}
	return t.maxSelf
}

func (t *Tree) Total() []int64 {
	if children, ok := t.Nodes[0]; ok && len(children) > 0 {
		total := make([]int64, len(children[0].Total))
		for _, child := range children {
			for i, childTotal := range child.Total {
				total[i] += childTotal
			}
		}
		return total
	}
	return []int64{0}
}

func (t *Tree) AddName(name string, nameHash uint64) {
	if _, exists := t.NamesMap[nameHash]; !exists {
		t.Names = append(t.Names, name)
		t.NamesMap[nameHash] = len(t.Names) - 1
	}
}

type TreeNodeV2 struct {
	FnID   uint64
	NodeID uint64
	Self   []int64
	Total  []int64
}

func (n *TreeNodeV2) Clone() *TreeNodeV2 {
	return &TreeNodeV2{
		FnID:   n.FnID,
		NodeID: n.NodeID,
		Self:   append([]int64(nil), n.Self...),
		Total:  append([]int64(nil), n.Total...),
	}
}

func (n *TreeNodeV2) SetTotalAndSelf(self []int64, total []int64) *TreeNodeV2 {
	res := n.Clone()
	res.Self = self
	res.Total = total
	return res
}

func (t *Tree) MergeTrie(nodes [][]any, functions [][]any, sampleType string) {
	sampleTypeIndex := -1
	for i, st := range t.SampleTypes {
		if st == sampleType {
			sampleTypeIndex = i
			break
		}
	}
	if sampleTypeIndex == -1 {
		return
	}

	for _, f := range functions {
		id := f[0].(uint64)
		fn := f[1].(string)
		if len(t.NamesMap) < 2_000_000 {
			if _, exists := t.NamesMap[id]; !exists {
				t.Names = append(t.Names, fn)
				t.NamesMap[id] = len(t.Names) - 1
			}
		}
	}

	for _, _n := range nodes {
		parentID := _n[0].(uint64)
		fnID := _n[1].(uint64)
		nodeID := _n[2].(uint64)
		selfValue := _n[3].(int64)
		totalValue := _n[4].(int64)

		if t.maxSelf[sampleTypeIndex] < selfValue {
			t.maxSelf[sampleTypeIndex] = selfValue
		}

		slf := make([]int64, len(t.SampleTypes))
		slf[sampleTypeIndex] = selfValue

		total := make([]int64, len(t.SampleTypes))
		total[sampleTypeIndex] = totalValue

		if children, ok := t.Nodes[parentID]; ok {
			if pos := findNode(nodeID, children); pos != -1 {
				node := children[pos].Clone()
				node.Self[sampleTypeIndex] += selfValue
				node.Total[sampleTypeIndex] += totalValue
				children[pos] = node
				continue
			}
		}

		if t.NodesNum >= 2_000_000 {
			return
		}

		t.Nodes[parentID] = append(t.Nodes[parentID], &TreeNodeV2{
			FnID:   fnID,
			NodeID: nodeID,
			Self:   slf,
			Total:  total,
		})

		t.NodesNum++
	}
}

func (t *Tree) BFS(sampleType string) []*prof.Level {
	sampleTypeIndex := -1
	for i, st := range t.SampleTypes {
		if st == sampleType {
			sampleTypeIndex = i
			break
		}
	}
	if sampleTypeIndex == -1 {
		return nil
	}

	res := make([]*prof.Level, 0)
	rootChildren := t.Nodes[0]

	var total int64
	for _, child := range rootChildren {
		total += child.Total[sampleTypeIndex]
	}

	res = append(res, &prof.Level{Values: []int64{0, total, 0, 0}})

	totals := make([]int64, len(t.SampleTypes))
	totals[sampleTypeIndex] = total

	totalNode := &TreeNodeV2{
		Self:   make([]int64, len(t.SampleTypes)),
		Total:  totals,
		NodeID: 0,
		FnID:   0,
	}

	prependMap := make(map[uint64]int64)
	reviewed := make(map[uint64]bool)

	currentLevelNodes := []*TreeNodeV2{totalNode}

	for len(currentLevelNodes) > 0 {
		var nextLevelNodes []*TreeNodeV2
		var prepend int64
		lvl := prof.Level{}

		for _, parent := range currentLevelNodes {
			prepend += prependMap[parent.NodeID]
			children, ok := t.Nodes[parent.NodeID]
			if !ok {
				prepend += parent.Total[sampleTypeIndex]
				continue
			}
			for _, child := range children {
				if reviewed[child.NodeID] {
					return res
				}
				reviewed[child.NodeID] = true

				prependMap[child.NodeID] = prepend
				nextLevelNodes = append(nextLevelNodes, child)

				lvl.Values = append(lvl.Values,
					prepend,
					child.Total[sampleTypeIndex],
					child.Self[sampleTypeIndex],
					int64(t.NamesMap[child.FnID]),
				)

				prepend = 0
			}

			prepend += parent.Self[sampleTypeIndex]
		}

		res = append(res, &lvl)
		currentLevelNodes = nextLevelNodes
	}

	return res
}

func synchronizeNames(t1, t2 *Tree) {
	// Synchronize names from t1 to t2
	namesToAddToT2 := make([]struct {
		id   uint64
		name string
	}, 0)

	for id, idx := range t1.NamesMap {
		if _, exists := t2.NamesMap[id]; !exists {
			namesToAddToT2 = append(namesToAddToT2, struct {
				id   uint64
				name string
			}{id, t1.Names[idx]})
		}
	}

	for _, nameData := range namesToAddToT2 {
		t2.AddName(nameData.name, nameData.id)
	}

	// Synchronize names from t2 to t1
	namesToAddToT1 := make([]struct {
		id   uint64
		name string
	}, 0)

	for id, idx := range t2.NamesMap {
		if _, exists := t1.NamesMap[id]; !exists {
			namesToAddToT1 = append(namesToAddToT1, struct {
				id   uint64
				name string
			}{id, t2.Names[idx]})
		}
	}

	for _, nameData := range namesToAddToT1 {
		t1.AddName(nameData.name, nameData.id)
	}
}

func assertPositive(t *Tree) bool {
	for _, nodes := range t.Nodes {
		for _, node := range nodes {
			for _, selfValue := range node.Self {
				if selfValue < 0 {
					return false
				}
			}
		}
	}
	return true
}

func mergeNodes(t1, t2 *Tree) {
	keys := make(map[uint64]struct{})
	for k := range t1.Nodes {
		keys[k] = struct{}{}
	}
	for k := range t2.Nodes {
		keys[k] = struct{}{}
	}

	for key := range keys {
		t1Children, ok1 := t1.Nodes[key]
		if !ok1 {
			t1Children = []*TreeNodeV2{}
		}
		t2Children, ok2 := t2.Nodes[key]
		if !ok2 {
			t2Children = []*TreeNodeV2{}
		}

		sort.Slice(t1Children, func(i, j int) bool {
			return t1Children[i].NodeID < t1Children[j].NodeID
		})
		sort.Slice(t2Children, func(i, j int) bool {
			return t2Children[i].NodeID < t2Children[j].NodeID
		})

		newT1Nodes, newT2Nodes := mergeChildren(t1Children, t2Children)
		t1.Nodes[key] = newT1Nodes
		t2.Nodes[key] = newT2Nodes
	}
}

func computeFlameGraphDiff(t1, t2 *Tree) *prof.FlameGraphDiff {
	res := &prof.FlameGraphDiff{}
	res.LeftTicks = t1.Total()[0]
	res.RightTicks = t2.Total()[0]
	res.Total = res.LeftTicks + res.RightTicks

	leftNodes := []*TreeNodeV2{{
		FnID:   0,
		NodeID: 0,
		Self:   []int64{0},
		Total:  []int64{res.LeftTicks},
	}}

	rightNodes := []*TreeNodeV2{{
		FnID:   0,
		NodeID: 0,
		Self:   []int64{0},
		Total:  []int64{res.RightTicks},
	}}

	levels := []int{0}
	xLeftOffsets := []int64{0}
	xRightOffsets := []int64{0}
	nameLocationCache := make(map[string]int64)

	for len(leftNodes) > 0 && len(rightNodes) > 0 {
		left := leftNodes[0]
		right := rightNodes[0]
		leftNodes = leftNodes[1:]
		rightNodes = rightNodes[1:]

		xLeftOffset := xLeftOffsets[0]
		xRightOffset := xRightOffsets[0]
		xLeftOffsets = xLeftOffsets[1:]
		xRightOffsets = xRightOffsets[1:]

		level := levels[0]
		levels = levels[1:]

		var name string
		if left.FnID == 0 {
			name = "total"
		} else {
			name = t1.Names[t1.NamesMap[left.FnID]]
		}

		nameIdx, ok := nameLocationCache[name]
		if !ok {
			nameIdx = int64(len(res.Names))
			res.Names = append(res.Names, name)
			nameLocationCache[name] = nameIdx
		}

		for len(res.Levels) <= level {
			res.Levels = append(res.Levels, &prof.Level{})
		}

		if res.MaxSelf < left.Self[0] {
			res.MaxSelf = left.Self[0]
		}
		if res.MaxSelf < right.Self[0] {
			res.MaxSelf = right.Self[0]
		}

		res.Levels[level].Values = append(res.Levels[level].Values,
			xLeftOffset, left.Total[0], left.Self[0],
			xRightOffset, right.Total[0], right.Self[0],
			nameIdx)

		if childrenLeft, ok := t1.Nodes[left.NodeID]; ok {
			childrenRight := t2.Nodes[right.NodeID]
			for i := len(childrenLeft) - 1; i >= 0; i-- {
				childLeft := childrenLeft[i]
				var childRight *TreeNodeV2
				if i < len(childrenRight) {
					childRight = childrenRight[i]
				} else {
					childRight = &TreeNodeV2{Self: []int64{0}, Total: []int64{0}}
				}
				leftNodes = append(leftNodes, childLeft)
				rightNodes = append(rightNodes, childRight)
				xLeftOffsets = append(xLeftOffsets, xLeftOffset)
				xRightOffsets = append(xRightOffsets, xRightOffset)
				xLeftOffset += childLeft.Total[0]
				xRightOffset += childRight.Total[0]
				levels = append(levels, level+1)
			}
		}
	}

	for i := range res.Levels {
		var prev0, prev3 int64
		for j := 0; j < len(res.Levels[i].Values); j += 7 {
			res.Levels[i].Values[j] -= prev0
			prev0 += res.Levels[i].Values[j] + res.Levels[i].Values[j+1]
			res.Levels[i].Values[j+3] -= prev3
			prev3 += res.Levels[i].Values[j+3] + res.Levels[i].Values[j+4]
		}
	}

	return res
}

func mergeChildren(t1Nodes, t2Nodes []*TreeNodeV2) ([]*TreeNodeV2, []*TreeNodeV2) {
	var newT1Nodes, newT2Nodes []*TreeNodeV2
	i, j := 0, 0

	for i < len(t1Nodes) && j < len(t2Nodes) {
		if t1Nodes[i].NodeID == t2Nodes[j].NodeID {
			newT1Nodes = append(newT1Nodes, t1Nodes[i].Clone())
			newT2Nodes = append(newT2Nodes, t2Nodes[j].Clone())
			i++
			j++
		} else if t1Nodes[i].NodeID < t2Nodes[j].NodeID {
			newT1Nodes = append(newT1Nodes, t1Nodes[i].Clone())
			newT2Nodes = append(newT2Nodes, createEmptyNode(t1Nodes[i]))
			i++
		} else {
			newT2Nodes = append(newT2Nodes, t2Nodes[j].Clone())
			newT1Nodes = append(newT1Nodes, createEmptyNode(t2Nodes[j]))
			j++
		}
	}

	for ; i < len(t1Nodes); i++ {
		newT1Nodes = append(newT1Nodes, t1Nodes[i].Clone())
		newT2Nodes = append(newT2Nodes, createEmptyNode(t1Nodes[i]))
	}

	for ; j < len(t2Nodes); j++ {
		newT2Nodes = append(newT2Nodes, t2Nodes[j].Clone())
		newT1Nodes = append(newT1Nodes, createEmptyNode(t2Nodes[j]))
	}

	return newT1Nodes, newT2Nodes
}

func createEmptyNode(node *TreeNodeV2) *TreeNodeV2 {
	return &TreeNodeV2{
		NodeID: node.NodeID,
		FnID:   node.FnID,
		Self:   make([]int64, len(node.Self)),
		Total:  make([]int64, len(node.Total)),
	}
}

func findNode(nodeID uint64, children []*TreeNodeV2) int {
	for i, child := range children {
		if child.NodeID == nodeID {
			return i
		}
	}
	return -1
}
