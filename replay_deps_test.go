package ruler

import (
	"sort"
	"testing"
	"time"

	"github.com/prometheus/common/model"
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

func mkRule(record, expr string) Rule {
	r := Rule{Record: record, Expr: expr}
	r.ID = HashRule(r)
	return r
}

func mkCfg(rules ...Rule) Config {
	interval := model.Duration(time.Minute)
	return Config{Groups: []Group{{Name: "g", Type: "prometheus", Interval: &interval, Rules: rules}}}
}

func TestBuildDepGraph_LinearChain(t *testing.T) {
	a := mkRule("A", "up")
	b := mkRule("B", "A")
	c := mkRule("C", "B")
	g, err := buildDepGraph(mkCfg(a, b, c))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(g.cycle) != 0 {
		t.Fatalf("cycle = %v, want empty", g.cycle)
	}
	posA, posB, posC := indexOf(g.order, a.ID), indexOf(g.order, b.ID), indexOf(g.order, c.ID)
	if !(posA < posB && posB < posC) {
		t.Errorf("order = %v, want A<B<C", g.order)
	}
}

func TestBuildDepGraph_ExternalRefsAreRoots(t *testing.T) {
	a := mkRule("A", `rate(external_metric[5m])`)
	g, err := buildDepGraph(mkCfg(a))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(g.nodes[a.ID].upstreams) != 0 {
		t.Errorf("upstreams = %v, want []", g.nodes[a.ID].upstreams)
	}
}

func TestBuildDepGraph_Cycle(t *testing.T) {
	a := mkRule("A", "B")
	b := mkRule("B", "A")
	g, err := buildDepGraph(mkCfg(a, b))
	if err != ErrReplayCycle {
		t.Fatalf("err = %v, want ErrReplayCycle", err)
	}
	if len(g.cycle) != 2 {
		t.Errorf("cycle = %v, want 2 members", g.cycle)
	}
}

func TestBuildDepGraph_SelfReference(t *testing.T) {
	a := mkRule("A", "A")
	g, err := buildDepGraph(mkCfg(a))
	if err != ErrReplayCycle {
		t.Fatalf("err = %v, want ErrReplayCycle", err)
	}
	if len(g.cycle) != 1 || g.cycle[0] != a.ID {
		t.Errorf("cycle = %v, want [A]", g.cycle)
	}
}

func TestBuildDepGraph_CrossGroup(t *testing.T) {
	interval := model.Duration(time.Minute)
	a := mkRule("A", "up")
	b := mkRule("B", "A")
	cfg := Config{Groups: []Group{
		{Name: "g1", Type: "prometheus", Interval: &interval, Rules: []Rule{a}},
		{Name: "g2", Type: "prometheus", Interval: &interval, Rules: []Rule{b}},
	}}
	g, err := buildDepGraph(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	ups := g.nodes[b.ID].upstreams
	if len(ups) != 1 || ups[0] != a.ID {
		t.Errorf("B upstreams = %v, want [A]", ups)
	}
}

func indexOf(s []uint64, target uint64) int {
	for i, v := range s {
		if v == target {
			return i
		}
	}
	return -1
}

func TestMaxRangeSelector_RateOnly(t *testing.T) {
	got, err := maxRangeSelector(`rate(foo[5m])`)
	if err != nil {
		t.Fatal(err)
	}
	if got != 5*time.Minute {
		t.Errorf("got %v, want 5m", got)
	}
}

func TestMaxRangeSelector_Subquery(t *testing.T) {
	got, err := maxRangeSelector(`avg_over_time(foo[7d:5m])`)
	if err != nil {
		t.Fatal(err)
	}
	if got != 7*24*time.Hour {
		t.Errorf("got %v, want 7d", got)
	}
}

func TestMaxRangeSelector_PicksMax(t *testing.T) {
	got, err := maxRangeSelector(`rate(foo[5m]) + avg_over_time(bar[30d:5m])`)
	if err != nil {
		t.Fatal(err)
	}
	if got != 30*24*time.Hour {
		t.Errorf("got %v, want 30d", got)
	}
}

func TestMaxRangeSelector_VectorOnly(t *testing.T) {
	got, err := maxRangeSelector(`up`)
	if err != nil {
		t.Fatal(err)
	}
	if got != 0 {
		t.Errorf("got %v, want 0", got)
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
