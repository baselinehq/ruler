package ruler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
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
