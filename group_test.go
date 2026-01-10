package ruler

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

// TestUpdateWithDuringEval verifies that UpdateWith() doesn't block when eval() is running.
// This is a regression test for the deadlock where Manager.Apply() would block indefinitely
// while a group's eval() was in progress.
func TestUpdateWithDuringEval(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	blockingQuerier := &mockBlockingQuerier{
		started: started,
		release: release,
	}

	// Create a manager
	mgr := &Manager{
		qb:                 blockingQuerier,
		writer:             &testNoopWriter{},
		ctx:                context.Background(),
		evaluationInterval: 10 * time.Second,
		logger:             &testLogger{t: t},
		groups:             make(map[uint64]*groupRunner),
	}

	// Create a group with a rule, no call Apply (to avoid auto-start)
	interval := model.Duration(10 * time.Second)
	cfg := Group{
		Type:     "prometheus",
		Name:     "test-group",
		File:     "test.yaml",
		Interval: &interval,
		Rules: []Rule{
			{
				Record: "test_metric",
				Expr:   "up",
			},
		},
	}

	// Build the group manually
	g, err := newGroupRunner(cfg, mgr)
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Start eval in background
	evalDone := make(chan struct{})
	go func() {
		defer close(evalDone)
		g.eval(context.Background(), time.Now())
	}()

	// Wait for Query to start (ensures eval is in progress)
	select {
	case <-started:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("query did not start in time")
	}

	// Create updated group
	cfg.Rules[0].Expr = "up{job=\"test\"}" // Modify the rule
	cfg.Checksum = "updated"               // Force checksum change
	newGroup, err := newGroupRunner(cfg, mgr)
	if err != nil {
		t.Fatalf("failed to create new group: %v", err)
	}

	// Now try to update while eval is running
	// This used to deadlock because UpdateWith() would block on the channel send
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- g.UpdateWith(newGroup)
	}()

	// The update should complete quickly (< 100ms) even though eval is blocked
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("UpdateWith failed: %v", err)
		}
		t.Logf("UpdateWith completed successfully while eval was running")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("UpdateWith() blocked for more than 100ms - deadlock detected!")
	}

	close(release)

	select {
	case <-evalDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("eval did not finish after release")
	}
}

// mockBlockingQuerier blocks Query until released.
type mockBlockingQuerier struct {
	started chan struct{}
	release chan struct{}
}

func (m *mockBlockingQuerier) Build(params QueryParams) Querier {
	return m
}

func (m *mockBlockingQuerier) Query(ctx context.Context, query string, ts time.Time) (Result, error) {
	select {
	case m.started <- struct{}{}:
	default:
	}
	select {
	case <-m.release:
		return Result{Data: []Metric{}}, nil
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

func (m *mockBlockingQuerier) QueryRange(ctx context.Context, query string, start, end time.Time) (Result, error) {
	return Result{Data: []Metric{}}, nil
}


// TestUpdateResetsRuleState verifies that rule updates reset per-rule state.
// This documents the current behavior where lastEvaluation and ruleState are
// lost on updates, meaning stale markers won't be emitted for series that
// existed before the update.
func TestUpdateResetsRuleState(t *testing.T) {
	eval1Results := Result{
		Data: []Metric{
			{
				Labels:     []prompb.Label{{Name: "series", Value: "A"}},
				Timestamps: []int64{100},
				Values:     []float64{1.0},
			},
			{
				Labels:     []prompb.Label{{Name: "series", Value: "B"}},
				Timestamps: []int64{100},
				Values:     []float64{2.0},
			},
		},
	}

	eval2Results := Result{
		Data: []Metric{
			{
				Labels:     []prompb.Label{{Name: "series", Value: "A"}},
				Timestamps: []int64{200},
				Values:     []float64{3.0},
			},
		},
	}

	querier := &mockConfigurableQuerier{
		results: []Result{eval1Results, eval2Results},
	}
	writer := &testCapturingWriter{}

	// Create a minimal manager
	mgr, err := NewManager(ManagerConfig{
		QuerierBuilder:     querier,
		Writer:             writer,
		Context:            context.Background(),
		EvaluationInterval: 10 * time.Second,
		Logger:             &testLogger{t: t},
	})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	interval := model.Duration(10 * time.Second)
	cfg := Config{
		Groups: []Group{
			{
				Type:     "prometheus",
				Name:     "test-group",
				File:     "test.yaml",
				Interval: &interval,
				Rules: []Rule{
					{
						Record: "test_metric",
						Expr:   "up",
					},
				},
			},
		},
	}

	// Apply initial config
	if err := mgr.Apply(cfg); err != nil {
		t.Fatalf("failed to apply config: %v", err)
	}

	// Get the group
	mgr.mu.RLock()
	var g *groupRunner
	for _, gr := range mgr.groups {
		g = gr
		break
	}
	mgr.mu.RUnlock()

	if g == nil {
		t.Fatal("no group found")
	}

	// First eval: establishes lastEvaluation with series A and B
	g.eval(context.Background(), time.Unix(100, 0))

	if len(writer.writes) != 1 {
		t.Fatalf("expected 1 write after first eval, got %d", len(writer.writes))
	}
	if len(writer.writes[0]) != 2 {
		t.Fatalf("expected 2 series in first eval, got %d", len(writer.writes[0]))
	}

	// Create a new group with updated config (e.g., add a label)
	cfg.Groups[0].Rules[0].Labels = map[string]string{"env": "prod"}
	cfg.Groups[0].Checksum = "updated" // Force checksum change
	newGroup, err := newGroupRunner(cfg.Groups[0], mgr)
	if err != nil {
		t.Fatalf("failed to create new group: %v", err)
	}

	// Apply the update directly
	g.applyUpdate(newGroup)

	// Second eval: only returns series A
	// Because applyUpdate() replaced g.rules with newGroup.rules (new RecordingRule objects),
	// the lastEvaluation state was reset, so NO stale marker for B will be emitted
	g.eval(context.Background(), time.Unix(200, 0))

	if len(writer.writes) != 2 {
		t.Fatalf("expected 2 writes total, got %d", len(writer.writes))
	}

	// The second write should only contain series A (no stale marker for B)
	// This demonstrates that lastEvaluation was reset on update
	secondWrite := writer.writes[1]
	if len(secondWrite) != 1 {
		t.Errorf("expected 1 series in second eval (only A, no stale B), got %d", len(secondWrite))

		// Show what we got
		for i, ts := range secondWrite {
			for _, lbl := range ts.Labels {
				if lbl.Name == "series" {
					t.Logf("  series %d: series=%s", i, lbl.Value)
				}
			}
		}

		// If we get 2 series, it means B got a stale marker, which would indicate
		// state continuity (not expected with current implementation)
	}

	// Verify the series is A with the new label
	foundA := false
	for _, ts := range secondWrite {
		hasSeriesA := false
		hasEnvLabel := false
		for _, lbl := range ts.Labels {
			if lbl.Name == "series" && lbl.Value == "A" {
				hasSeriesA = true
			}
			if lbl.Name == "env" && lbl.Value == "prod" {
				hasEnvLabel = true
			}
		}
		if hasSeriesA && hasEnvLabel {
			foundA = true
		}
	}
	if !foundA {
		t.Error("expected series A with env=prod label in second eval")
	}

	mgr.Stop()
}

// mockConfigurableQuerier returns different results for each Query call
type mockConfigurableQuerier struct {
	results []Result
	index   int
}

func (m *mockConfigurableQuerier) Build(params QueryParams) Querier {
	return m
}

func (m *mockConfigurableQuerier) Query(ctx context.Context, query string, ts time.Time) (Result, error) {
	if m.index >= len(m.results) {
		return Result{Data: []Metric{}}, nil
	}
	result := m.results[m.index]
	m.index++
	return result, nil
}

func (m *mockConfigurableQuerier) QueryRange(ctx context.Context, query string, start, end time.Time) (Result, error) {
	return Result{Data: []Metric{}}, nil
}
