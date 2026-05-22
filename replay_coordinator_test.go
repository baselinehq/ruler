package ruler

import (
	"context"
	"testing"
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
