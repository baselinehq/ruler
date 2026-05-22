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
	c, err := newReplayCoordinator(context.Background(), ReplayConfig{Enabled: true, DefaultSpan: 1}, &testQuerier{}, &testNoopWriter{}, &testLogger{t: t}, nil)
	if err != nil {
		t.Fatalf("newReplayCoordinator: %v", err)
	}
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
	c, err := newReplayCoordinator(context.Background(), ReplayConfig{Enabled: true, DefaultSpan: 1}, &testQuerier{}, &testNoopWriter{}, &testLogger{t: t}, nil)
	if err != nil {
		t.Fatalf("newReplayCoordinator: %v", err)
	}
	defer c.Stop()
	c.setOutcome(20, OutcomeFailed)
	r := &replayRunner{coord: c, upstreams: []chan struct{}{up}}
	if err := r.waitUpstreams(context.Background(), []uint64{20}); err == nil {
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

func TestProbeCoverage_DisabledReturnsFullSpan(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	resumeFrom := now.Add(-30 * 24 * time.Hour)
	r := &replayRunner{cfg: ReplayConfig{ProbeOutput: false}, groupCfg: nil}
	step := 5 * time.Minute
	got, err := r.probeCoverage(context.Background(), resumeFrom, now, step)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %v, want one full-span gap", got)
	}
	if !got[0].Start.Equal(resumeFrom) || !got[0].End.Equal(now) {
		t.Errorf("gap = %v", got[0])
	}
}

func TestProbeCoverage_EmptyMatrixIsOneGap(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	resumeFrom := now.Add(-1 * time.Hour)
	r := &replayRunner{
		cfg:      ReplayConfig{ProbeOutput: true, GapMergeWindow: 0},
		groupCfg: nil,
		q:        &testQuerier{result: Result{}},
		rule:     Rule{Record: "rec"},
	}
	got, err := r.probeCoverage(context.Background(), resumeFrom, now, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Errorf("got %d gaps, want 1", len(got))
	}
}

func TestPlanChunks_SingleGapMultipleChunks(t *testing.T) {
	t0 := time.Unix(0, 0)
	gaps := []timeRange{{Start: t0, End: t0.Add(24 * time.Hour)}}
	chunks := planChunks(gaps, 6*time.Hour)
	if len(chunks) != 4 {
		t.Errorf("got %d chunks, want 4", len(chunks))
	}
	if !chunks[0].Start.Equal(t0) || !chunks[0].End.Equal(t0.Add(6*time.Hour)) {
		t.Errorf("chunk0 = %v", chunks[0])
	}
	if !chunks[3].End.Equal(t0.Add(24 * time.Hour)) {
		t.Errorf("last chunk end = %v, want 24h", chunks[3].End)
	}
}

func TestPlanChunks_HandlesShortTrailingChunk(t *testing.T) {
	t0 := time.Unix(0, 0)
	gaps := []timeRange{{Start: t0, End: t0.Add(7 * time.Hour)}}
	chunks := planChunks(gaps, 6*time.Hour)
	if len(chunks) != 2 {
		t.Fatalf("got %d, want 2", len(chunks))
	}
	if !chunks[1].End.Equal(t0.Add(7 * time.Hour)) {
		t.Errorf("trailing end = %v, want 7h", chunks[1].End)
	}
}

func TestPlanChunks_MultipleGaps(t *testing.T) {
	t0 := time.Unix(0, 0)
	gaps := []timeRange{
		{Start: t0, End: t0.Add(6 * time.Hour)},
		{Start: t0.Add(20 * time.Hour), End: t0.Add(26 * time.Hour)},
	}
	chunks := planChunks(gaps, 6*time.Hour)
	if len(chunks) != 2 {
		t.Errorf("got %d, want 2", len(chunks))
	}
}

func TestExecChunk_WritesResultViaWriter(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	wr := &testCapturingWriter{}
	q := &testQuerier{result: Result{Data: []Metric{{
		Labels:     []prompb.Label{{Name: "k", Value: "v"}},
		Timestamps: []int64{now.UnixMilli()},
		Values:     []float64{42},
	}}}}
	r := &replayRunner{
		cfg:    ReplayConfig{ChunkTimeout: time.Second, Concurrency: 1},
		rule:   Rule{Record: "out"},
		q:      q,
		writer: wr,
	}
	chunk := timeRange{Start: now.Add(-time.Hour), End: now}
	if err := r.execChunk(context.Background(), chunk); err != nil {
		t.Fatal(err)
	}
	if len(wr.writes) != 1 || len(wr.writes[0]) != 1 {
		t.Errorf("writes = %v", wr.writes)
	}
}

func TestExecChunk_EmptyResultSkipsWrite(t *testing.T) {
	wr := &testCapturingWriter{}
	r := &replayRunner{
		cfg:    ReplayConfig{ChunkTimeout: time.Second, Concurrency: 1},
		rule:   Rule{Record: "out"},
		q:      &testQuerier{result: Result{}},
		writer: wr,
	}
	if err := r.execChunk(context.Background(), timeRange{}); err != nil {
		t.Fatal(err)
	}
	if len(wr.writes) != 0 {
		t.Errorf("got writes %v, want none", wr.writes)
	}
}

func TestEmitProgressMarker_WritesExpectedSeries(t *testing.T) {
	wr := &testCapturingWriter{}
	r := &replayRunner{
		cfg:    ReplayConfig{ProgressMetric: "ruler_replay_progress"},
		rule:   Rule{ID: 7, Record: "out"},
		writer: wr,
		logger: &testLogger{t: t},
	}
	chunkEnd := time.Unix(1_700_000_000, 0)
	r.emitProgressMarker(context.Background(), chunkEnd)
	if len(wr.writes) != 1 || len(wr.writes[0]) != 1 {
		t.Fatalf("writes = %v", wr.writes)
	}
	ts := wr.writes[0][0]
	labels := map[string]string{}
	for _, l := range ts.Labels {
		labels[l.Name] = l.Value
	}
	if labels["__name__"] != "ruler_replay_progress" {
		t.Errorf("__name__ = %q", labels["__name__"])
	}
	if labels["rule_id"] != "7" {
		t.Errorf("rule_id = %q", labels["rule_id"])
	}
	if labels["record"] != "out" {
		t.Errorf("record = %q", labels["record"])
	}
	if ts.Samples[0].Value != float64(chunkEnd.Unix()) {
		t.Errorf("value = %v, want %v", ts.Samples[0].Value, float64(chunkEnd.Unix()))
	}
}

func TestEmitProgressMarker_DisabledNoop(t *testing.T) {
	wr := &testCapturingWriter{}
	r := &replayRunner{cfg: ReplayConfig{ProgressMetric: ""}, writer: wr}
	r.emitProgressMarker(context.Background(), time.Now())
	if len(wr.writes) != 0 {
		t.Errorf("got writes %v, want none", wr.writes)
	}
}

func TestRunnerRun_HappyPath(t *testing.T) {
	now := time.Now()
	wr := &testCapturingWriter{}
	q := &testQuerier{result: Result{Data: []Metric{{
		Labels:     []prompb.Label{{Name: "k", Value: "v"}},
		Timestamps: []int64{now.Add(-2 * time.Hour).UnixMilli()},
		Values:     []float64{1},
	}}}}
	c, err := newReplayCoordinator(context.Background(), ReplayConfig{
		Enabled:       true,
		DefaultSpan:   1 * time.Hour,
		BatchInterval: 30 * time.Minute,
		ChunkTimeout:  time.Second,
		Concurrency:   1,
		ProbeOutput:   false,
	}, q, wr, &testLogger{t: t}, nil)
	if err != nil {
		t.Fatalf("newReplayCoordinator: %v", err)
	}
	defer c.Stop()
	r := &replayRunner{
		rule:   Rule{Record: "out", Expr: "rate(foo[5m])", ID: 1},
		cfg:    c.cfg,
		q:      q,
		writer: wr,
		coord:  c,
		logger: &testLogger{t: t},
		done:   c.doneCh(1),
	}
	r.Run(context.Background())
	if got := c.outcome(1); got != OutcomeCompleted {
		t.Errorf("outcome = %v, want completed", got)
	}
	if len(wr.writes) == 0 {
		t.Error("no writes captured")
	}
}

var _ = time.Now
