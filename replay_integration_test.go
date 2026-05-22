package ruler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

func invServer(metrics ...string) *httptest.Server {
	body := `{"status":"success","data":["` + strings.Join(metrics, `","`) + `"]}`
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
}

func TestReplayIntegration_HappyPath(t *testing.T) {
	now := time.Now().Truncate(time.Minute)
	srv := invServer("foo")
	defer srv.Close()
	client, _ := NewHTTPClient(HTTPConfig{URL: srv.URL, Timeout: time.Second})

	q := &testRangeQuerier{
		responses: map[string][]Metric{
			"rate(foo[5m])": {{
				Labels:     []prompb.Label{{Name: "k", Value: "v"}},
				Timestamps: makeRangeTimestamps(now.Add(-1*time.Hour), now, 15*time.Minute),
				Values:     []float64{1, 1, 1, 1, 1},
			}},
		},
	}
	wr := &testCapturingWriter{}

	cfg := mustParseConfigBytes(t, []byte(`
groups:
  - name: g
    interval: 30m
    replay:
      enabled: true
      span: 1h
    rules:
      - record: out_metric
        expr: rate(foo[5m])
`))
	mgr, err := NewManager(ManagerConfig{
		QuerierBuilder: q, Writer: wr, Context: context.Background(),
		EvaluationInterval: time.Hour, Logger: &testLogger{t: t},
		Replay: &ReplayConfig{
			Enabled: true, DefaultSpan: time.Hour, BatchInterval: 30 * time.Minute,
			ChunkTimeout: time.Second, Concurrency: 1, RulesConcurrency: 2,
			ProbeOutput: false,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mgr.replay.httpClient = client
	defer mgr.Stop()
	if err := mgr.Apply(cfg); err != nil {
		t.Fatal(err)
	}

	ruleID := cfg.Groups[0].Rules[0].ID
	waitOutcome(t, mgr.replay, ruleID, 5*time.Second)

	if got := mgr.replay.outcome(ruleID); got != OutcomeCompleted {
		t.Errorf("outcome = %v, want completed", got)
	}
	if len(wr.writes) == 0 {
		t.Error("no writes captured")
	}
}

func makeRangeTimestamps(start, end time.Time, step time.Duration) []int64 {
	out := []int64{}
	for t := start; t.Before(end); t = t.Add(step) {
		out = append(out, t.UnixMilli())
	}
	return out
}

func waitOutcome(t *testing.T, c *replayCoordinator, ruleID uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.outcome(ruleID) != OutcomePending {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestReplayIntegration_AlreadyBackfilled(t *testing.T) {
	srv := invServer("foo")
	defer srv.Close()
	client, _ := NewHTTPClient(HTTPConfig{URL: srv.URL, Timeout: time.Second})

	q := &testRangeQuerier{
		probeFunc: func(query string, start, end time.Time) (Result, error) {
			if query == "count(out_metric)" {
				ts := makeRangeTimestamps(start, end, 30*time.Minute)
				ts = append(ts, end.UnixMilli())
				vals := make([]float64, len(ts))
				for i := range vals {
					vals[i] = 1
				}
				return Result{Data: []Metric{{
					Labels:     []prompb.Label{{Name: "k", Value: "v"}},
					Timestamps: ts,
					Values:     vals,
				}}}, nil
			}
			return Result{}, nil
		},
	}
	wr := &testCapturingWriter{}

	cfg := mustParseConfigBytes(t, []byte(`
groups:
  - name: g
    interval: 30m
    replay:
      enabled: true
      span: 1h
    rules:
      - record: out_metric
        expr: rate(foo[5m])
`))
	mgr, err := NewManager(ManagerConfig{
		QuerierBuilder: q, Writer: wr, Context: context.Background(),
		EvaluationInterval: time.Hour, Logger: &testLogger{t: t},
		Replay: &ReplayConfig{
			Enabled: true, DefaultSpan: time.Hour, BatchInterval: 30 * time.Minute,
			ChunkTimeout: time.Second, Concurrency: 1, RulesConcurrency: 2,
			ProbeOutput: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mgr.replay.httpClient = client
	defer mgr.Stop()
	if err := mgr.Apply(cfg); err != nil {
		t.Fatal(err)
	}

	ruleID := cfg.Groups[0].Rules[0].ID
	waitOutcome(t, mgr.replay, ruleID, 5*time.Second)

	if got := mgr.replay.outcome(ruleID); got != OutcomeSkippedAlreadyBackfilled {
		t.Errorf("outcome = %v, want skipped_already_backfilled", got)
	}
	if len(wr.writes) != 0 {
		t.Errorf("writes = %d, want 0", len(wr.writes))
	}
}

func TestReplayIntegration_PartialGap(t *testing.T) {
	srv := invServer("foo")
	defer srv.Close()
	client, _ := NewHTTPClient(HTTPConfig{URL: srv.URL, Timeout: time.Second})

	q := &testRangeQuerier{
		probeFunc: func(query string, start, end time.Time) (Result, error) {
			switch query {
			case "count(out_metric)":
				ts := []int64{start.UnixMilli(), start.Add(15 * time.Minute).UnixMilli()}
				vals := []float64{1, 1}
				return Result{Data: []Metric{{
					Labels:     []prompb.Label{{Name: "k", Value: "v"}},
					Timestamps: ts,
					Values:     vals,
				}}}, nil
			case "rate(foo[5m])":
				ts := makeRangeTimestamps(start, end, 15*time.Minute)
				vals := make([]float64, len(ts))
				for i := range vals {
					vals[i] = 1
				}
				return Result{Data: []Metric{{
					Labels:     []prompb.Label{{Name: "k", Value: "v"}},
					Timestamps: ts,
					Values:     vals,
				}}}, nil
			}
			return Result{}, nil
		},
	}
	wr := &testCapturingWriter{}

	cfg := mustParseConfigBytes(t, []byte(`
groups:
  - name: g
    interval: 30m
    replay:
      enabled: true
      span: 1h
    rules:
      - record: out_metric
        expr: rate(foo[5m])
`))
	mgr, err := NewManager(ManagerConfig{
		QuerierBuilder: q, Writer: wr, Context: context.Background(),
		EvaluationInterval: time.Hour, Logger: &testLogger{t: t},
		Replay: &ReplayConfig{
			Enabled: true, DefaultSpan: time.Hour, BatchInterval: 30 * time.Minute,
			ChunkTimeout: time.Second, Concurrency: 1, RulesConcurrency: 2,
			ProbeOutput: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mgr.replay.httpClient = client
	defer mgr.Stop()
	if err := mgr.Apply(cfg); err != nil {
		t.Fatal(err)
	}

	ruleID := cfg.Groups[0].Rules[0].ID
	waitOutcome(t, mgr.replay, ruleID, 5*time.Second)

	if got := mgr.replay.outcome(ruleID); got != OutcomeCompleted {
		t.Errorf("outcome = %v, want completed", got)
	}
	if len(wr.writes) == 0 {
		t.Fatal("no writes captured")
	}

	now := time.Now()
	spanMidMillis := now.Add(-30 * time.Minute).UnixMilli()
	foundSecondHalf := false
	for _, batch := range wr.writes {
		for _, ts := range batch {
			for _, sample := range ts.Samples {
				if sample.Timestamp >= spanMidMillis {
					foundSecondHalf = true
				}
			}
		}
	}
	if !foundSecondHalf {
		t.Error("expected at least one sample in the second half of the span")
	}
}

func TestReplayIntegration_CycleDetectionCascade(t *testing.T) {
	srv := invServer()
	defer srv.Close()
	client, _ := NewHTTPClient(HTTPConfig{URL: srv.URL, Timeout: time.Second})

	q := &testRangeQuerier{}
	wr := &testCapturingWriter{}

	cfg := mustParseConfigBytes(t, []byte(`
groups:
  - name: g
    interval: 30m
    replay:
      enabled: true
      span: 1h
    rules:
      - record: A
        expr: B
      - record: B
        expr: A
      - record: C
        expr: A
`))
	mgr, err := NewManager(ManagerConfig{
		QuerierBuilder: q, Writer: wr, Context: context.Background(),
		EvaluationInterval: time.Hour, Logger: &testLogger{t: t},
		Replay: &ReplayConfig{
			Enabled: true, DefaultSpan: time.Hour, BatchInterval: 30 * time.Minute,
			ChunkTimeout: time.Second, Concurrency: 1, RulesConcurrency: 2,
			ProbeOutput: false,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mgr.replay.httpClient = client
	defer mgr.Stop()
	if err := mgr.Apply(cfg); err != nil {
		t.Fatal(err)
	}

	idA := cfg.Groups[0].Rules[0].ID
	idB := cfg.Groups[0].Rules[1].ID
	idC := cfg.Groups[0].Rules[2].ID

	waitOutcome(t, mgr.replay, idA, 5*time.Second)
	waitOutcome(t, mgr.replay, idB, 5*time.Second)
	waitOutcome(t, mgr.replay, idC, 5*time.Second)

	if got := mgr.replay.outcome(idA); got != OutcomeCycle {
		t.Errorf("outcome(A) = %v, want cycle", got)
	}
	if got := mgr.replay.outcome(idB); got != OutcomeCycle {
		t.Errorf("outcome(B) = %v, want cycle", got)
	}
	if got := mgr.replay.outcome(idC); got == OutcomeCompleted {
		t.Errorf("outcome(C) = %v, expected non-success (cycle cascade)", got)
	}
}

func TestReplayIntegration_ConcurrentLiveEval(t *testing.T) {
	srv := invServer("foo")
	defer srv.Close()
	client, _ := NewHTTPClient(HTTPConfig{URL: srv.URL, Timeout: time.Second})

	q := &testRangeQuerier{
		probeFunc: func(query string, start, end time.Time) (Result, error) {
			ts := makeRangeTimestamps(start, end, 15*time.Minute)
			vals := make([]float64, len(ts))
			for i := range vals {
				vals[i] = 1
			}
			return Result{Data: []Metric{{
				Labels:     []prompb.Label{{Name: "k", Value: "v"}},
				Timestamps: ts,
				Values:     vals,
			}}}, nil
		},
		instantFunc: func(query string, ts time.Time) (Result, error) {
			return Result{Data: []Metric{{
				Labels:     []prompb.Label{{Name: "k", Value: "v"}},
				Timestamps: []int64{ts.UnixMilli()},
				Values:     []float64{1},
			}}}, nil
		},
	}
	wr := &testCountingWriter{notify: make(chan struct{}, 1)}

	cfg := mustParseConfigBytes(t, []byte(`
groups:
  - name: g
    interval: 50ms
    replay:
      enabled: true
      span: 1h
    rules:
      - record: out_metric
        expr: rate(foo[5m])
`))
	mgr, err := NewManager(ManagerConfig{
		QuerierBuilder: q, Writer: wr, Context: context.Background(),
		EvaluationInterval: 50 * time.Millisecond,
		MaxStartDelay:      10 * time.Millisecond,
		Logger:             &testLogger{t: t},
		Replay: &ReplayConfig{
			Enabled: true, DefaultSpan: time.Hour, BatchInterval: 30 * time.Minute,
			ChunkTimeout: time.Second, Concurrency: 1, RulesConcurrency: 1,
			ProbeOutput: false,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mgr.replay.httpClient = client
	defer mgr.Stop()
	if err := mgr.Apply(cfg); err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	if got := atomic.LoadInt32(&wr.count); got < 2 {
		t.Errorf("write count = %d, want >= 2", got)
	}

	ruleID := cfg.Groups[0].Rules[0].ID
	waitOutcome(t, mgr.replay, ruleID, 5*time.Second)
	if got := mgr.replay.outcome(ruleID); got != OutcomeCompleted {
		t.Errorf("outcome = %v, want completed", got)
	}
}

func TestReplayIntegration_CancellationMidReplay(t *testing.T) {
	srv := invServer("foo")
	defer srv.Close()
	client, _ := NewHTTPClient(HTTPConfig{URL: srv.URL, Timeout: 5 * time.Second})

	var chunkCount int32
	q := &testRangeQuerier{
		probeFunc: func(query string, start, end time.Time) (Result, error) {
			atomic.AddInt32(&chunkCount, 1)
			time.Sleep(50 * time.Millisecond)
			ts := makeRangeTimestamps(start, end, 15*time.Minute)
			vals := make([]float64, len(ts))
			for i := range vals {
				vals[i] = 1
			}
			return Result{Data: []Metric{{
				Labels:     []prompb.Label{{Name: "k", Value: "v"}},
				Timestamps: ts,
				Values:     vals,
			}}}, nil
		},
	}
	wr := &testCapturingWriter{}

	cfg := mustParseConfigBytes(t, []byte(`
groups:
  - name: g
    interval: 1h
    replay:
      enabled: true
      span: 4h
    rules:
      - record: out_metric
        expr: rate(foo[5m])
`))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr, err := NewManager(ManagerConfig{
		QuerierBuilder: q, Writer: wr, Context: ctx,
		EvaluationInterval: time.Hour, Logger: &testLogger{t: t},
		Replay: &ReplayConfig{
			Enabled: true, DefaultSpan: 4 * time.Hour, BatchInterval: 30 * time.Minute,
			ChunkTimeout: 5 * time.Second, Concurrency: 1, RulesConcurrency: 1,
			ProbeOutput: false,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mgr.replay.httpClient = client
	defer mgr.Stop()
	if err := mgr.Apply(cfg); err != nil {
		t.Fatal(err)
	}

	time.AfterFunc(100*time.Millisecond, cancel)

	ruleID := cfg.Groups[0].Rules[0].ID
	waitOutcome(t, mgr.replay, ruleID, 5*time.Second)

	got := mgr.replay.outcome(ruleID)
	if got != OutcomeCancelled && got != OutcomeFailed {
		t.Errorf("outcome = %v, want cancelled or failed", got)
	}
	final := atomic.LoadInt32(&chunkCount)
	if final < 1 {
		t.Errorf("chunkCount = %d, want >= 1", final)
	}
	if final >= 8 {
		t.Errorf("chunkCount = %d, want < 8 (cancellation should have prevented all chunks)", final)
	}
}

func TestReplayIntegration_ChunkRetrySuccess(t *testing.T) {
	srv := invServer("foo")
	defer srv.Close()
	client, _ := NewHTTPClient(HTTPConfig{URL: srv.URL, Timeout: 5 * time.Second})

	var attempts int32
	q := &testRangeQuerier{
		probeFunc: func(query string, start, end time.Time) (Result, error) {
			n := atomic.AddInt32(&attempts, 1)
			if n <= 2 {
				return Result{}, errors.New(`status=503 body="upstream"`)
			}
			ts := makeRangeTimestamps(start, end, 15*time.Minute)
			vals := make([]float64, len(ts))
			for i := range vals {
				vals[i] = 1
			}
			return Result{Data: []Metric{{
				Labels:     []prompb.Label{{Name: "k", Value: "v"}},
				Timestamps: ts,
				Values:     vals,
			}}}, nil
		},
	}
	wr := &testCapturingWriter{}

	cfg := mustParseConfigBytes(t, []byte(`
groups:
  - name: g
    interval: 30m
    replay:
      enabled: true
      span: 30m
    rules:
      - record: out_metric
        expr: rate(foo[5m])
`))
	mgr, err := NewManager(ManagerConfig{
		QuerierBuilder: q, Writer: wr, Context: context.Background(),
		EvaluationInterval: time.Hour, Logger: &testLogger{t: t},
		Replay: &ReplayConfig{
			Enabled: true, DefaultSpan: 30 * time.Minute, BatchInterval: 30 * time.Minute,
			ChunkTimeout: 10 * time.Second, Concurrency: 1, RulesConcurrency: 1,
			ProbeOutput: false,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mgr.replay.httpClient = client
	defer mgr.Stop()
	if err := mgr.Apply(cfg); err != nil {
		t.Fatal(err)
	}

	ruleID := cfg.Groups[0].Rules[0].ID
	waitOutcome(t, mgr.replay, ruleID, 15*time.Second)

	if got := mgr.replay.outcome(ruleID); got != OutcomeCompleted {
		t.Errorf("outcome = %v, want completed", got)
	}
	if got := atomic.LoadInt32(&attempts); got <= 2 {
		t.Errorf("attempts = %d, want > 2 (expected at least one retry observed)", got)
	}
}

func TestReplayIntegration_ChunkRetryExhausted(t *testing.T) {
	srv := invServer("foo")
	defer srv.Close()
	client, _ := NewHTTPClient(HTTPConfig{URL: srv.URL, Timeout: 5 * time.Second})

	q := &testRangeQuerier{
		probeFunc: func(query string, start, end time.Time) (Result, error) {
			return Result{}, errors.New(`status=503 body="upstream"`)
		},
	}
	wr := &testCapturingWriter{}

	cfg := mustParseConfigBytes(t, []byte(`
groups:
  - name: g
    interval: 30m
    replay:
      enabled: true
      span: 30m
    rules:
      - record: out_metric
        expr: rate(foo[5m])
`))
	mgr, err := NewManager(ManagerConfig{
		QuerierBuilder: q, Writer: wr, Context: context.Background(),
		EvaluationInterval: time.Hour, Logger: &testLogger{t: t},
		Replay: &ReplayConfig{
			Enabled: true, DefaultSpan: 30 * time.Minute, BatchInterval: 30 * time.Minute,
			ChunkTimeout: 10 * time.Second, Concurrency: 1, RulesConcurrency: 1,
			ProbeOutput: false,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mgr.replay.httpClient = client
	defer mgr.Stop()
	if err := mgr.Apply(cfg); err != nil {
		t.Fatal(err)
	}

	ruleID := cfg.Groups[0].Rules[0].ID
	waitOutcome(t, mgr.replay, ruleID, 15*time.Second)

	if got := mgr.replay.outcome(ruleID); got != OutcomeFailed {
		t.Errorf("outcome = %v, want failed", got)
	}
}

func TestReplayIntegration_SourceRetentionClamp(t *testing.T) {
	t.Skip("source retention detection not yet wired into Runner.Run (deferred to follow-up)")
}

func TestReplayIntegration_EmptyMatcher(t *testing.T) {
	srv := invServer("foo")
	defer srv.Close()
	client, _ := NewHTTPClient(HTTPConfig{URL: srv.URL, Timeout: time.Second})

	q := &testRangeQuerier{
		probeFunc: func(query string, start, end time.Time) (Result, error) {
			return Result{}, nil
		},
	}
	wr := &testCapturingWriter{}

	cfg := mustParseConfigBytes(t, []byte(`
groups:
  - name: g
    interval: 30m
    replay:
      enabled: true
      span: 30m
    rules:
      - record: out_metric
        expr: rate(foo[5m])
`))
	mgr, err := NewManager(ManagerConfig{
		QuerierBuilder: q, Writer: wr, Context: context.Background(),
		EvaluationInterval: time.Hour, Logger: &testLogger{t: t},
		Replay: &ReplayConfig{
			Enabled: true, DefaultSpan: 30 * time.Minute, BatchInterval: 30 * time.Minute,
			ChunkTimeout: time.Second, Concurrency: 1, RulesConcurrency: 1,
			ProbeOutput: false,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mgr.replay.httpClient = client
	defer mgr.Stop()
	if err := mgr.Apply(cfg); err != nil {
		t.Fatal(err)
	}

	ruleID := cfg.Groups[0].Rules[0].ID
	waitOutcome(t, mgr.replay, ruleID, 5*time.Second)

	if got := mgr.replay.outcome(ruleID); got != OutcomeCompleted {
		t.Errorf("outcome = %v, want completed", got)
	}
	if len(wr.writes) != 0 {
		t.Errorf("writes = %d, want 0", len(wr.writes))
	}
}

func TestReplayIntegration_InventoryFetchFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()
	client, _ := NewHTTPClient(HTTPConfig{URL: srv.URL, Timeout: time.Second})

	q := &testRangeQuerier{}
	wr := &testCapturingWriter{}

	cfg := mustParseConfigBytes(t, []byte(`
groups:
  - name: g
    interval: 30m
    replay:
      enabled: true
      span: 1h
    rules:
      - record: out_metric
        expr: rate(foo[5m])
`))
	mgr, err := NewManager(ManagerConfig{
		QuerierBuilder: q, Writer: wr, Context: context.Background(),
		EvaluationInterval: time.Hour, Logger: &testLogger{t: t},
		Replay: &ReplayConfig{
			Enabled: true, DefaultSpan: time.Hour, BatchInterval: 30 * time.Minute,
			ChunkTimeout: time.Second, Concurrency: 1, RulesConcurrency: 1,
			ProbeOutput: false,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mgr.replay.httpClient = client
	defer mgr.Stop()
	if err := mgr.Apply(cfg); err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	ruleID := cfg.Groups[0].Rules[0].ID
	if got := mgr.replay.outcome(ruleID); got != OutcomePending {
		t.Errorf("outcome = %v, want pending (runner never spawned due to inventory failure)", got)
	}
}

func TestReplayIntegration_ApplyReturnsImmediately(t *testing.T) {
	srv := invServer("foo")
	defer srv.Close()
	client, _ := NewHTTPClient(HTTPConfig{URL: srv.URL, Timeout: time.Second})
	probeStarted := make(chan struct{}, 1)
	releaseProbe := make(chan struct{})
	q := &testRangeQuerier{
		probeFunc: func(query string, start, end time.Time) (Result, error) {
			select {
			case probeStarted <- struct{}{}:
			default:
			}
			<-releaseProbe
			return Result{}, nil
		},
		responses: map[string][]Metric{},
	}
	wr := &testCapturingWriter{}
	cfg := mustParseConfigBytes(t, []byte(`
groups:
  - name: g
    interval: 30m
    replay: { enabled: true, span: 1h }
    rules:
      - record: out
        expr: rate(foo[5m])
`))
	mgr, _ := NewManager(ManagerConfig{
		QuerierBuilder: q, Writer: wr, Context: context.Background(),
		EvaluationInterval: time.Hour, Logger: &testLogger{t: t},
		Replay: &ReplayConfig{Enabled: true, DefaultSpan: time.Hour, BatchInterval: 30 * time.Minute, ChunkTimeout: time.Second, RulesConcurrency: 1, Concurrency: 1, ProbeOutput: true},
	})
	mgr.replay.httpClient = client
	start := time.Now()
	if err := mgr.Apply(cfg); err != nil {
		t.Fatal(err)
	}
	if dur := time.Since(start); dur > 100*time.Millisecond {
		t.Errorf("Apply took %v, want <100ms", dur)
	}

	select {
	case <-probeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("probe did not start within 2s")
	}
	close(releaseProbe)
	ruleID := cfg.Groups[0].Rules[0].ID
	waitOutcome(t, mgr.replay, ruleID, 5*time.Second)
	mgr.Stop()
}

func TestReplayIntegration_RuleLabelsPropagate(t *testing.T) {
	now := time.Now().Truncate(time.Minute)
	srv := invServer("foo")
	defer srv.Close()
	client, _ := NewHTTPClient(HTTPConfig{URL: srv.URL, Timeout: time.Second})

	q := &testRangeQuerier{
		responses: map[string][]Metric{
			"rate(foo[5m])": {{
				Labels:     []prompb.Label{{Name: "instance", Value: "i1"}},
				Timestamps: makeRangeTimestamps(now.Add(-1*time.Hour), now, 15*time.Minute),
				Values:     []float64{1, 1, 1, 1, 1},
			}},
		},
	}
	wr := &testCapturingWriter{}

	cfg := mustParseConfigBytes(t, []byte(`
groups:
  - name: g
    interval: 30m
    labels:
      cluster: east
    replay: { enabled: true, span: 1h }
    rules:
      - record: out_metric
        expr: rate(foo[5m])
        labels:
          tier: prod
`))

	mgr, err := NewManager(ManagerConfig{
		QuerierBuilder:     q,
		Writer:             wr,
		Context:            context.Background(),
		EvaluationInterval: time.Hour,
		Logger:             &testLogger{t: t},
		ExternalLabels:     map[string]string{"region": "us"},
		Replay: &ReplayConfig{
			Enabled: true, DefaultSpan: time.Hour, BatchInterval: 30 * time.Minute,
			ChunkTimeout: time.Second, Concurrency: 1, RulesConcurrency: 2,
			ProbeOutput: false,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mgr.replay.httpClient = client
	defer mgr.Stop()
	if err := mgr.Apply(cfg); err != nil {
		t.Fatal(err)
	}
	ruleID := cfg.Groups[0].Rules[0].ID
	waitOutcome(t, mgr.replay, ruleID, 5*time.Second)

	if len(wr.writes) == 0 || len(wr.writes[0]) == 0 {
		t.Fatal("no writes captured")
	}
	labels := map[string]string{}
	for _, l := range wr.writes[0][0].Labels {
		labels[l.Name] = l.Value
	}
	for _, want := range []struct{ k, v string }{
		{"__name__", "out_metric"},
		{"tier", "prod"},
		{"cluster", "east"},
		{"region", "us"},
	} {
		if labels[want.k] != want.v {
			t.Errorf("label %q = %q, want %q (full: %v)", want.k, labels[want.k], want.v, labels)
		}
	}
}

func TestReplayIntegration_GroupReplayDisabled(t *testing.T) {
	srv := invServer("foo")
	defer srv.Close()
	client, _ := NewHTTPClient(HTTPConfig{URL: srv.URL, Timeout: time.Second})

	q := &testRangeQuerier{}
	wr := &testCapturingWriter{}

	cfg := mustParseConfigBytes(t, []byte(`
groups:
  - name: g
    interval: 30m
    replay:
      enabled: false
    rules:
      - record: out_metric
        expr: rate(foo[5m])
`))
	mgr, err := NewManager(ManagerConfig{
		QuerierBuilder: q, Writer: wr, Context: context.Background(),
		EvaluationInterval: time.Hour, Logger: &testLogger{t: t},
		Replay: &ReplayConfig{
			Enabled: true, DefaultSpan: time.Hour, BatchInterval: 30 * time.Minute,
			ChunkTimeout: time.Second, Concurrency: 1, RulesConcurrency: 1,
			ProbeOutput: false,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mgr.replay.httpClient = client
	defer mgr.Stop()
	if err := mgr.Apply(cfg); err != nil {
		t.Fatal(err)
	}

	ruleID := cfg.Groups[0].Rules[0].ID
	waitOutcome(t, mgr.replay, ruleID, 5*time.Second)

	if got := mgr.replay.outcome(ruleID); got != OutcomeSkippedDisabled {
		t.Errorf("outcome = %v, want skipped_disabled", got)
	}
	if len(wr.writes) != 0 {
		t.Errorf("writes = %d, want 0", len(wr.writes))
	}
}
