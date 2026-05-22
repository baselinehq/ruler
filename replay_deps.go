package ruler

import (
	"errors"
	"fmt"

	"github.com/prometheus/prometheus/promql/parser"
)

// ErrReplayCycle signals one or more cycles in the rule dependency graph.
// Cycle members are reported in depGraph.cycle. The acyclic subset is still
// available via depGraph.order.
var ErrReplayCycle = errors.New("replay: dependency cycle detected")

type depNode struct {
	ruleID    uint64
	upstreams []uint64
	done      chan struct{}
}

type depGraph struct {
	nodes map[uint64]*depNode
	order []uint64
	cycle []uint64
}

// buildDepGraph constructs the dependency graph across all groups in cfg.
// Returns ErrReplayCycle if cycles exist; acyclic subset is still populated.
func buildDepGraph(cfg Config) (*depGraph, error) {
	recordNames := map[string]uint64{}
	allRules := map[uint64]Rule{}
	for _, grp := range cfg.Groups {
		for _, r := range grp.Rules {
			if r.Record == "" {
				continue
			}
			recordNames[r.Record] = r.ID
			allRules[r.ID] = r
		}
	}

	nodes := make(map[uint64]*depNode, len(allRules))
	for id, r := range allRules {
		selectors, err := extractSelectors(r.Expr)
		if err != nil {
			return nil, fmt.Errorf("rule %q: %w", r.Record, err)
		}
		var ups []uint64
		for _, name := range selectors {
			if upID, ok := recordNames[name]; ok && upID != id {
				ups = append(ups, upID)
			} else if name == r.Record {
				ups = append(ups, id)
			}
		}
		nodes[id] = &depNode{ruleID: id, upstreams: ups, done: make(chan struct{})}
	}

	order, cycle := kahnTopoSort(nodes)
	g := &depGraph{nodes: nodes, order: order, cycle: cycle}
	if len(cycle) > 0 {
		return g, ErrReplayCycle
	}
	return g, nil
}

func kahnTopoSort(nodes map[uint64]*depNode) (order, cycle []uint64) {
	inDegree := make(map[uint64]int, len(nodes))
	dependents := make(map[uint64][]uint64, len(nodes))
	for id, n := range nodes {
		inDegree[id] = len(n.upstreams)
		for _, up := range n.upstreams {
			dependents[up] = append(dependents[up], id)
		}
	}
	queue := make([]uint64, 0)
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}
	order = make([]uint64, 0, len(nodes))
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		order = append(order, id)
		for _, d := range dependents[id] {
			inDegree[d]--
			if inDegree[d] == 0 {
				queue = append(queue, d)
			}
		}
	}
	if len(order) < len(nodes) {
		inOrder := make(map[uint64]struct{}, len(order))
		for _, id := range order {
			inOrder[id] = struct{}{}
		}
		for id := range nodes {
			if _, ok := inOrder[id]; !ok {
				cycle = append(cycle, id)
			}
		}
	}
	return order, cycle
}

// extractSelectors returns the unique metric names referenced by `expr`.
// Both instant (VectorSelector) and range (MatrixSelector wraps a VectorSelector)
// selectors are captured. Walks the AST via parser.Walk.
func extractSelectors(expr string) ([]string, error) {
	tree, err := parser.ParseExpr(expr)
	if err != nil {
		return nil, fmt.Errorf("parse expr: %w", err)
	}
	seen := map[string]struct{}{}
	parser.Inspect(tree, func(node parser.Node, _ []parser.Node) error {
		if vs, ok := node.(*parser.VectorSelector); ok && vs.Name != "" {
			seen[vs.Name] = struct{}{}
		}
		return nil
	})
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	return out, nil
}
