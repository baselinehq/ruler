package ruler

import (
	"testing"
	"time"
)

func TestAdjustReqTimestamp_EvalDelayAlignment(t *testing.T) {
	interval := time.Minute
	evalDelay := 30 * time.Second
	g := &groupRunner{
		interval:      interval,
		evalDelay:     evalDelay,
		evalAlignment: nil, // default = aligned
	}

	evalTS := time.Date(2024, 1, 1, 12, 0, 45, 0, time.UTC)
	got := g.queryTimestamp(evalTS)
	want := evalTS.Add(-evalDelay).Truncate(interval)
	if !got.Equal(want) {
		t.Fatalf("query timestamp = %v, want %v", got, want)
	}
}

func TestAdjustReqTimestamp_NoAlignment(t *testing.T) {
	interval := time.Minute
	evalDelay := 30 * time.Second
	alignment := false
	g := &groupRunner{
		interval:      interval,
		evalDelay:     evalDelay,
		evalAlignment: &alignment,
	}

	evalTS := time.Date(2024, 1, 1, 12, 0, 45, 0, time.UTC)
	got := g.queryTimestamp(evalTS)
	want := evalTS.Add(-evalDelay)
	if !got.Equal(want) {
		t.Fatalf("query timestamp = %v, want %v", got, want)
	}
}

func TestEvalOffsetSemantics(t *testing.T) {
	evalDelay := 30 * time.Second
	offset := 15 * time.Second
	g := &groupRunner{
		evalDelay:  evalDelay,
		evalOffset: &offset,
	}

	evalTS := time.Date(2024, 1, 1, 12, 0, 15, 0, time.UTC)
	got := g.queryTimestamp(evalTS)
	if !got.Equal(evalTS) {
		t.Fatalf("query timestamp = %v, want %v (eval_offset should skip eval_delay)", got, evalTS)
	}
}
