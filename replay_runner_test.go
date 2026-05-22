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

// Touch context import (used in later runner tests)
var _ = context.Background
var _ = time.Now
