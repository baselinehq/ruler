package ruler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

func TestReplayWritesRangeResultsWithGroupConfig(t *testing.T) {
	now := time.Now()
	qb := &capturingRangeBuilder{
		result: Result{Data: []Metric{{
			Labels:     []prompb.Label{{Name: "__name__", Value: "up"}, {Name: "instance", Value: "a"}},
			Timestamps: []int64{now.Add(-10 * time.Minute).UnixMilli()},
			Values:     []float64{1},
		}}},
	}
	wr := &testCapturingWriter{}
	mgr, err := NewManager(ManagerConfig{
		QuerierBuilder:     qb,
		Writer:             wr,
		Context:            context.Background(),
		EvaluationInterval: time.Minute,
		ExternalLabels:     map[string]string{"cluster": "prod"},
		Logger:             &testLogger{t: t},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Stop()

	interval := model.Duration(time.Minute)
	cfg := Config{Groups: []Group{{
		Name:     "g",
		Type:     "prometheus",
		Interval: &interval,
		Labels:   map[string]string{"team": "core"},
		Params:   Params{"nocache": []string{"1"}},
		Headers:  []Header{{Key: "X-Scope-OrgID", Value: "tenant-a"}},
		Rules:    []Rule{mkRule("out_metric", "rate(up[5m])")},
	}}}

	if err := mgr.Replay(context.Background(), cfg, ReplayOptions{
		TimeFrom:              now.Add(-20 * time.Minute),
		TimeTo:                now.Add(-10 * time.Minute),
		MaxDatapointsPerQuery: 10,
		RuleRetryAttempts:     1,
	}); err != nil {
		t.Fatal(err)
	}
	if len(qb.params) == 0 {
		t.Fatal("querier was not built")
	}
	params := qb.params[0]
	if params.EvaluationInterval != time.Minute {
		t.Errorf("EvaluationInterval = %v, want 1m", params.EvaluationInterval)
	}
	if got := params.QueryParams.Get("nocache"); got != "1" {
		t.Errorf("nocache param = %q, want 1", got)
	}
	if got := params.Headers["X-Scope-OrgID"]; got != "tenant-a" {
		t.Errorf("header = %q, want tenant-a", got)
	}
	if len(wr.writes) == 0 || len(wr.writes[0]) == 0 {
		t.Fatal("no replay output written")
	}

	labels := make(map[string]string, len(wr.writes[0][0].Labels))
	for _, label := range wr.writes[0][0].Labels {
		labels[label.Name] = label.Value
	}
	if labels["__name__"] != "out_metric" {
		t.Errorf("__name__ = %q, want out_metric", labels["__name__"])
	}
	if labels["team"] != "core" {
		t.Errorf("team = %q, want core", labels["team"])
	}
	if labels["cluster"] != "prod" {
		t.Errorf("cluster = %q, want prod", labels["cluster"])
	}
}

func TestReplayChunksByIntervalAndMaxDatapoints(t *testing.T) {
	start := time.Unix(1000, 0)
	end := start.Add(25 * time.Minute)
	qb := &capturingRangeBuilder{}
	wr := &testCapturingWriter{}
	r, err := newReplay(ReplayOptions{
		TimeFrom:              start,
		TimeTo:                end,
		MaxDatapointsPerQuery: 10,
	}, replayTestDeps(qb, wr, &testLogger{t: t}, nil))
	if err != nil {
		t.Fatal(err)
	}

	interval := model.Duration(time.Minute)
	cfg := Config{Groups: []Group{{
		Name:     "g",
		Type:     "prometheus",
		Interval: &interval,
		Rules:    []Rule{mkRule("out_metric", "rate(up[5m])")},
	}}}
	if err := r.run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}

	want := []rangeCall{
		{start: start, end: start.Add(10 * time.Minute)},
		{start: start.Add(10 * time.Minute), end: start.Add(20 * time.Minute)},
		{start: start.Add(20 * time.Minute), end: end},
	}
	if len(qb.ranges) != len(want) {
		t.Fatalf("range query count = %d, want %d", len(qb.ranges), len(want))
	}
	for i := range want {
		if !qb.ranges[i].start.Equal(want[i].start) || !qb.ranges[i].end.Equal(want[i].end) {
			t.Fatalf("range query %d = [%v,%v], want [%v,%v]", i, qb.ranges[i].start, qb.ranges[i].end, want[i].start, want[i].end)
		}
	}
}

func TestReplayRejectsDuplicateGroupIdentityBeforeWriting(t *testing.T) {
	wr := &testCapturingWriter{}
	r, err := newReplay(ReplayOptions{
		TimeFrom:              time.Unix(1000, 0),
		TimeTo:                time.Unix(2000, 0),
		MaxDatapointsPerQuery: 10,
	}, replayTestDeps(&testQuerier{}, wr, &testLogger{t: t}, nil))
	if err != nil {
		t.Fatal(err)
	}

	interval := model.Duration(time.Minute)
	group := Group{
		Name:     "g",
		Type:     "prometheus",
		Interval: &interval,
		Rules:    []Rule{mkRule("out_metric", "rate(up[5m])")},
	}
	err = r.run(context.Background(), Config{Groups: []Group{group, group}})
	if err == nil {
		t.Fatal("want duplicate group identity error")
	}
	if !strings.Contains(err.Error(), "duplicate group identity") {
		t.Fatalf("error = %v, want duplicate group identity", err)
	}
	if len(wr.writes) != 0 {
		t.Fatalf("writes = %d, want none", len(wr.writes))
	}
}

func TestReplayHonorsGroupLimit(t *testing.T) {
	now := time.Now()
	qb := &testQuerier{
		result: Result{Data: []Metric{
			{
				Labels:     []prompb.Label{{Name: "__name__", Value: "up"}, {Name: "instance", Value: "a"}},
				Timestamps: []int64{now.Add(-10 * time.Minute).UnixMilli()},
				Values:     []float64{1},
			},
			{
				Labels:     []prompb.Label{{Name: "__name__", Value: "up"}, {Name: "instance", Value: "b"}},
				Timestamps: []int64{now.Add(-10 * time.Minute).UnixMilli()},
				Values:     []float64{1},
			},
		}},
	}
	wr := &testCapturingWriter{}
	r, err := newReplay(ReplayOptions{
		TimeFrom:              now.Add(-time.Hour),
		TimeTo:                now.Add(-10 * time.Minute),
		RuleRetryAttempts:     1,
		MaxDatapointsPerQuery: 10,
	}, replayTestDeps(qb, wr, &testLogger{t: t}, nil))
	if err != nil {
		t.Fatal(err)
	}

	interval := model.Duration(time.Minute)
	limit := 1
	cfg := Config{Groups: []Group{{
		Name:     "g",
		Type:     "prometheus",
		Interval: &interval,
		Limit:    &limit,
		Rules:    []Rule{mkRule("out_metric", "rate(up[5m])")},
	}}}
	err = r.run(context.Background(), cfg)
	if err == nil {
		t.Fatal("want limit error")
	}
	if !strings.Contains(err.Error(), "exec exceeded limit of 1 with 2 series") {
		t.Fatalf("error = %v, want limit error", err)
	}
	if len(wr.writes) != 0 {
		t.Fatalf("writes = %d, want none", len(wr.writes))
	}
}

type capturingRangeBuilder struct {
	params []QueryParams
	result Result
	ranges []rangeCall
}

type rangeCall struct {
	start time.Time
	end   time.Time
}

func (b *capturingRangeBuilder) Build(params QueryParams) Querier {
	b.params = append(b.params, params)
	return &capturingRangeQuerier{builder: b}
}

type capturingRangeQuerier struct {
	builder *capturingRangeBuilder
}

func (q *capturingRangeQuerier) Query(ctx context.Context, query string, ts time.Time) (Result, error) {
	return q.builder.result, nil
}

func (q *capturingRangeQuerier) QueryRange(ctx context.Context, query string, start, end time.Time) (Result, error) {
	q.builder.ranges = append(q.builder.ranges, rangeCall{start: start, end: end})
	return q.builder.result, nil
}

func replayTestDeps(qb QuerierBuilder, writer SeriesWriter, logger Logger, externalLabels map[string]string) groupRunnerDeps {
	return groupRunnerDeps{
		qb:                 qb,
		writer:             writer,
		logger:             logger,
		externalLabels:     externalLabels,
		evaluationInterval: time.Minute,
	}
}
