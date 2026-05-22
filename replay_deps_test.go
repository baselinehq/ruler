package ruler

import (
	"sort"
	"testing"
)

func TestExtractSelectors_VectorSelector(t *testing.T) {
	names, err := extractSelectors("up")
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if !equalSorted(names, []string{"up"}) {
		t.Errorf("got %v, want [up]", names)
	}
}

func TestExtractSelectors_MatrixSelector(t *testing.T) {
	names, err := extractSelectors(`rate(container_cpu_usage_seconds_total[5m])`)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if !equalSorted(names, []string{"container_cpu_usage_seconds_total"}) {
		t.Errorf("got %v, want [container_cpu_usage_seconds_total]", names)
	}
}

func TestExtractSelectors_Subquery(t *testing.T) {
	names, err := extractSelectors(`avg_over_time(baseline:foo:rate5m[7d:5m])`)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if !equalSorted(names, []string{"baseline:foo:rate5m"}) {
		t.Errorf("got %v, want [baseline:foo:rate5m]", names)
	}
}

func TestExtractSelectors_Binary(t *testing.T) {
	names, err := extractSelectors(`foo / bar`)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if !equalSorted(names, []string{"bar", "foo"}) {
		t.Errorf("got %v, want [bar foo]", names)
	}
}

func TestExtractSelectors_AggregationWithGrouping(t *testing.T) {
	names, err := extractSelectors(`sum by (x) (rate(foo[5m]))`)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if !equalSorted(names, []string{"foo"}) {
		t.Errorf("got %v, want [foo]", names)
	}
}

func TestExtractSelectors_DedupesRepeatedRefs(t *testing.T) {
	names, err := extractSelectors(`foo + foo + rate(foo[5m])`)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if !equalSorted(names, []string{"foo"}) {
		t.Errorf("got %v, want [foo]", names)
	}
}

func TestExtractSelectors_RejectsParseError(t *testing.T) {
	if _, err := extractSelectors("not a + valid("); err == nil {
		t.Fatal("want parse error")
	}
}

func equalSorted(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	g := append([]string(nil), got...)
	w := append([]string(nil), want...)
	sort.Strings(g)
	sort.Strings(w)
	for i := range g {
		if g[i] != w[i] {
			return false
		}
	}
	return true
}
