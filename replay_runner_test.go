package ruler

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

func TestToTimeSeriesMatrix_PreservesHistoricalTimestamps(t *testing.T) {
	r := &replayRunner{rule: Rule{Record: "out_metric"}}
	res := Result{Data: []Metric{{
		Labels:     []prompb.Label{{Name: "k", Value: "v"}},
		Timestamps: []int64{1000, 2000, 3000},
		Values:     []float64{1.1, 2.2, 3.3},
	}}}
	got := r.toTimeSeriesMatrix(res)
	if len(got) != 1 {
		t.Fatalf("got %d series, want 1", len(got))
	}
	if len(got[0].Samples) != 3 {
		t.Fatalf("got %d samples, want 3", len(got[0].Samples))
	}
	wantTimestamps := []int64{1000, 2000, 3000}
	for i, s := range got[0].Samples {
		if s.Timestamp != wantTimestamps[i] {
			t.Errorf("sample[%d].Timestamp = %d, want %d", i, s.Timestamp, wantTimestamps[i])
		}
	}
	var nameVal string
	for _, l := range got[0].Labels {
		if l.Name == "__name__" {
			nameVal = l.Value
		}
	}
	if nameVal != "out_metric" {
		t.Errorf("__name__ = %q, want out_metric", nameVal)
	}
}

func TestToTimeSeriesMatrix_MergesRuleLabels(t *testing.T) {
	r := &replayRunner{
		rule:       Rule{Record: "out"},
		ruleLabels: []prompb.Label{{Name: "tier", Value: "prod"}},
	}
	res := Result{Data: []Metric{{
		Labels:     []prompb.Label{{Name: "tier", Value: "staging"}},
		Timestamps: []int64{1000},
		Values:     []float64{1},
	}}}
	got := r.toTimeSeriesMatrix(res)
	want := map[string]string{"__name__": "out", "tier": "prod", "exported_tier": "staging"}
	gotMap := map[string]string{}
	for _, l := range got[0].Labels {
		gotMap[l.Name] = l.Value
	}
	for k, v := range want {
		if gotMap[k] != v {
			t.Errorf("label %q = %q, want %q", k, gotMap[k], v)
		}
	}
}

func TestValidateSources_AllowsRecordedRefs(t *testing.T) {
	inv := &metricInventory{knownNames: map[string]struct{}{}}
	records := map[string]uint64{"baseline:foo:rate5m": 1}
	r := &replayRunner{rule: Rule{Expr: `avg_over_time(baseline:foo:rate5m[7d:5m])`}}
	if err := r.validateSources(inv, records); err != nil {
		t.Errorf("err = %v, want nil (recorded ref)", err)
	}
}

func TestValidateSources_RejectsMissingExternal(t *testing.T) {
	inv := &metricInventory{knownNames: map[string]struct{}{}}
	records := map[string]uint64{}
	r := &replayRunner{rule: Rule{Expr: `rate(container_cpu_usage_seconds_total[5m])`}}
	if err := r.validateSources(inv, records); err == nil {
		t.Error("want error for missing external metric")
	}
}

func TestValidateSources_AllowsKnownExternal(t *testing.T) {
	inv := &metricInventory{knownNames: map[string]struct{}{"container_cpu_usage_seconds_total": {}}}
	records := map[string]uint64{}
	r := &replayRunner{rule: Rule{Expr: `rate(container_cpu_usage_seconds_total[5m])`}}
	if err := r.validateSources(inv, records); err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

// Touch context import (used in later runner tests)
var _ = context.Background
var _ = time.Now
