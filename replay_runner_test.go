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

func TestWaitUpstreams_AllSuccessReturnsNil(t *testing.T) {
	up1 := make(chan struct{})
	close(up1)
	up2 := make(chan struct{})
	close(up2)
	c, _ := newReplayCoordinator(context.Background(), ReplayConfig{Enabled: true, DefaultSpan: 1}, &testQuerier{}, &testNoopWriter{}, &testLogger{t: t})
	defer c.Stop()
	c.setOutcome(10, OutcomeCompleted)
	c.setOutcome(11, OutcomeCompleted)
	r := &replayRunner{coord: c, upstreams: []chan struct{}{up1, up2}}
	if err := r.waitUpstreams(context.Background(), []uint64{10, 11}); err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

func TestWaitUpstreams_FailedUpstreamCascades(t *testing.T) {
	up := make(chan struct{})
	close(up)
	c, _ := newReplayCoordinator(context.Background(), ReplayConfig{Enabled: true, DefaultSpan: 1}, &testQuerier{}, &testNoopWriter{}, &testLogger{t: t})
	defer c.Stop()
	c.setOutcome(20, OutcomeFailed)
	r := &replayRunner{coord: c, upstreams: []chan struct{}{up}}
	err := r.waitUpstreams(context.Background(), []uint64{20})
	if err == nil {
		t.Fatal("want error on failed upstream")
	}
}

func TestWaitUpstreams_CtxCancel(t *testing.T) {
	up := make(chan struct{}) // never closes
	r := &replayRunner{upstreams: []chan struct{}{up}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.waitUpstreams(ctx, []uint64{30}); err == nil {
		t.Error("want ctx error")
	}
}

func TestReadProgressMarker_DisabledReturnsZero(t *testing.T) {
	r := &replayRunner{cfg: ReplayConfig{ProgressMetric: ""}}
	got, err := r.readProgressMarker(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !got.IsZero() {
		t.Errorf("got %v, want zero", got)
	}
}

func TestReadProgressMarker_ReturnsWatermark(t *testing.T) {
	wantSec := int64(1700000000)
	r := &replayRunner{
		cfg:  ReplayConfig{ProgressMetric: "ruler_replay_progress"},
		rule: Rule{ID: 99, Record: "rec"},
		q: &testQuerier{result: Result{Data: []Metric{{
			Labels:     []prompb.Label{{Name: "rule_id", Value: "99"}},
			Timestamps: []int64{wantSec * 1000},
			Values:     []float64{float64(wantSec)},
		}}}},
	}
	got, err := r.readProgressMarker(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Unix() != wantSec {
		t.Errorf("watermark = %v, want unix=%d", got, wantSec)
	}
}

var _ = time.Now
