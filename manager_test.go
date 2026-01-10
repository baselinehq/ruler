package ruler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

func TestGroupRemovalStopsRunner(t *testing.T) {
	writer := &testCountingWriter{
		notify: make(chan struct{}, 1),
	}
	querier := &testQuerier{
		result: Result{
			Data: []Metric{
				{
					Labels:     []prompb.Label{{Name: "series", Value: "A"}},
					Timestamps: []int64{time.Now().UnixMilli()},
					Values:     []float64{1},
				},
			},
		},
	}

	mgr, err := NewManager(ManagerConfig{
		QuerierBuilder:     querier,
		Writer:             writer,
		Context:            context.Background(),
		EvaluationInterval: 10 * time.Millisecond,
		MaxStartDelay:      5 * time.Millisecond,
		Logger:             &testLogger{t: t},
	})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	interval := model.Duration(10 * time.Millisecond)
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

	if err := mgr.Apply(cfg); err != nil {
		t.Fatalf("failed to apply config: %v", err)
	}

	// Wait for at least one write
	select {
	case <-writer.notify:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected at least one write")
	}

	if err := mgr.Apply(Config{}); err != nil {
		t.Fatalf("failed to remove groups: %v", err)
	}

	before := atomic.LoadInt32(&writer.count)

	select {
	case <-writer.notify:
		t.Fatal("unexpected write after group removal")
	case <-time.After(50 * time.Millisecond):
	}

	after := atomic.LoadInt32(&writer.count)
	if after != before {
		t.Fatalf("writes increased after removal: before=%d after=%d", before, after)
	}

	mgr.Stop()
}

