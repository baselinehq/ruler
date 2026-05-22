package ruler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

func TestCoordinator_DisabledReturnsNil(t *testing.T) {
	c, err := newReplayCoordinator(context.Background(), ReplayConfig{Enabled: false}, &testQuerier{}, &testNoopWriter{}, &testLogger{t: t})
	if err != nil {
		t.Fatal(err)
	}
	if c != nil {
		t.Errorf("c = %v, want nil when disabled", c)
	}
}

func TestCoordinator_MarkProbedDedup(t *testing.T) {
	c, err := newReplayCoordinator(context.Background(), ReplayConfig{Enabled: true, DefaultSpan: 1}, &testQuerier{}, &testNoopWriter{}, &testLogger{t: t})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	if !c.markProbed(42) {
		t.Error("first markProbed = false, want true")
	}
	if c.markProbed(42) {
		t.Error("second markProbed = true, want false")
	}
}

func TestCoordinator_SetOutcomeClosesDoneChan(t *testing.T) {
	c, _ := newReplayCoordinator(context.Background(), ReplayConfig{Enabled: true, DefaultSpan: 1}, &testQuerier{}, &testNoopWriter{}, &testLogger{t: t})
	defer c.Stop()
	ch := c.doneCh(7)
	c.setOutcome(7, OutcomeCompleted)
	select {
	case <-ch:
	default:
		t.Error("done not closed after setOutcome")
	}
	if got := c.outcome(7); got != OutcomeCompleted {
		t.Errorf("outcome = %v, want completed", got)
	}
}

func TestCoordinator_SetOutcomeBeforeDoneCh_StillCascades(t *testing.T) {
	c, _ := newReplayCoordinator(context.Background(), ReplayConfig{Enabled: true, DefaultSpan: time.Hour}, &testQuerier{}, &testNoopWriter{}, &testLogger{t: t})
	defer c.Stop()
	c.setOutcome(42, OutcomeSkippedDisabled)
	ch := c.doneCh(42)
	select {
	case <-ch:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("done channel not closed when outcome set before doneCh creation")
	}
}

func TestCoordinator_UpdateProgressMonotonic(t *testing.T) {
	c, _ := newReplayCoordinator(context.Background(), ReplayConfig{Enabled: true, DefaultSpan: 1}, &testQuerier{}, &testNoopWriter{}, &testLogger{t: t})
	defer c.Stop()
	c.updateProgress(1, 100)
	c.updateProgress(1, 50) // older, should be ignored
	c.updateProgress(1, 200)
	if got := c.progress[1]; got != 200 {
		t.Errorf("progress = %d, want 200", got)
	}
}

func TestCoordinatorOnApply_SpawnsRunnerForNewRule(t *testing.T) {
	invSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":["up","foo"]}`))
	}))
	defer invSrv.Close()
	client, _ := NewHTTPClient(HTTPConfig{URL: invSrv.URL, Timeout: time.Second})

	wr := &testCapturingWriter{}
	q := &queryAdapter{HTTPClient: client, result: Result{Data: []Metric{{
		Labels:     []prompb.Label{{Name: "k", Value: "v"}},
		Timestamps: []int64{time.Now().UnixMilli()},
		Values:     []float64{1},
	}}}}

	c, err := newReplayCoordinator(context.Background(), ReplayConfig{
		Enabled: true, DefaultSpan: 1 * time.Hour, BatchInterval: 30 * time.Minute,
		ChunkTimeout: time.Second, Concurrency: 1, RulesConcurrency: 2,
	}, q, wr, &testLogger{t: t})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	c.httpClient = client

	interval := model.Duration(time.Minute)
	cfg := Config{Groups: []Group{{
		Name: "g", Type: "prometheus", Interval: &interval,
		Rules: []Rule{mkRule("out", "rate(foo[5m])")},
	}}}

	c.OnApply(context.Background(), cfg)
	// Wait for outcome (timeout-bounded).
	deadline := time.Now().Add(5 * time.Second)
	ruleID := cfg.Groups[0].Rules[0].ID
	for time.Now().Before(deadline) {
		if c.outcome(ruleID) != OutcomePending {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !c.outcome(ruleID).IsSuccess() {
		t.Errorf("outcome = %v", c.outcome(ruleID))
	}
}

// queryAdapter combines an HTTPClient (for inventory) with canned Querier responses.
type queryAdapter struct {
	*HTTPClient
	result Result
	err    error
}

func (q *queryAdapter) Build(params QueryParams) Querier { return q }
func (q *queryAdapter) Query(ctx context.Context, query string, ts time.Time) (Result, error) {
	return q.result, q.err
}
func (q *queryAdapter) QueryRange(ctx context.Context, query string, start, end time.Time) (Result, error) {
	return q.result, q.err
}
