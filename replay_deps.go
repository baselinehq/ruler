package ruler

import (
	"fmt"

	"github.com/prometheus/prometheus/promql/parser"
)

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
