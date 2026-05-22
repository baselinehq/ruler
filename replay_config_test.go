package ruler

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v3"
)

func TestGroupReplayConfig_YAMLRoundTrip(t *testing.T) {
	yamlIn := []byte(`
groups:
  - name: g
    interval: 1m
    replay:
      enabled: true
      span: 30d
      max_lookback: 90d
    rules:
      - record: foo
        expr: up
`)
	cfg, err := ParseConfig(yamlIn)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Groups) != 1 || cfg.Groups[0].Replay == nil {
		t.Fatalf("replay block lost")
	}
	r := cfg.Groups[0].Replay
	if r.Enabled == nil || *r.Enabled != true {
		t.Errorf("enabled = %v, want true", r.Enabled)
	}
	if r.Span == nil || time.Duration(*r.Span) != 30*24*time.Hour {
		t.Errorf("span = %v, want 30d", r.Span)
	}
	if r.MaxLookback == nil || time.Duration(*r.MaxLookback) != 90*24*time.Hour {
		t.Errorf("max_lookback = %v, want 90d", r.MaxLookback)
	}

	out, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	round, err := ParseConfig(out)
	if err != nil {
		t.Fatalf("reparse: %v", err)
	}
	if len(round.Groups) != 1 || round.Groups[0].Replay == nil {
		t.Fatalf("replay block lost on round trip")
	}
	rr := round.Groups[0].Replay
	if rr.Enabled == nil || *rr.Enabled != true {
		t.Errorf("round-trip enabled = %v, want true", rr.Enabled)
	}
	if rr.Span == nil || time.Duration(*rr.Span) != 30*24*time.Hour {
		t.Errorf("round-trip span = %v, want 30d", rr.Span)
	}
	if rr.MaxLookback == nil || time.Duration(*rr.MaxLookback) != 90*24*time.Hour {
		t.Errorf("round-trip max_lookback = %v, want 90d", rr.MaxLookback)
	}
}

func TestGroupReplayConfig_ValidateRejectsNegativeSpan(t *testing.T) {
	neg := model.Duration(-1 * time.Hour)
	g := &GroupReplayConfig{Span: &neg}
	if err := g.Validate(); err == nil {
		t.Fatal("want error for negative span")
	}
}

func TestGroupReplayConfig_ValidateRejectsMaxLookbackBelowSpan(t *testing.T) {
	span := model.Duration(30 * 24 * time.Hour)
	max := model.Duration(7 * 24 * time.Hour)
	g := &GroupReplayConfig{Span: &span, MaxLookback: &max}
	if err := g.Validate(); err == nil {
		t.Fatal("want error when max_lookback < span")
	}
}

func TestGroupReplayConfig_NilSafe(t *testing.T) {
	var g *GroupReplayConfig
	if err := g.Validate(); err != nil {
		t.Errorf("nil should not error: %v", err)
	}
}

func TestReplayConfig_AppliesDefaults(t *testing.T) {
	got := ReplayConfig{Enabled: true}.applyDefaults()
	if got.BatchInterval != 6*time.Hour {
		t.Errorf("BatchInterval = %v, want 6h", got.BatchInterval)
	}
	if got.Concurrency != 2 {
		t.Errorf("Concurrency = %d, want 2", got.Concurrency)
	}
	if got.RulesConcurrency != 4 {
		t.Errorf("RulesConcurrency = %d, want 4", got.RulesConcurrency)
	}
	if got.ChunkTimeout != 5*time.Minute {
		t.Errorf("ChunkTimeout = %v, want 5m", got.ChunkTimeout)
	}
	if got.GapMergeWindow != 12*time.Hour {
		t.Errorf("GapMergeWindow = %v, want 2 * BatchInterval", got.GapMergeWindow)
	}
}

func TestReplayConfig_ValidateRejectsBadProgressMetric(t *testing.T) {
	c := ReplayConfig{ProgressMetric: "not a metric name"}
	if err := c.validate(); err == nil {
		t.Fatal("want error for invalid metric name")
	}
}

func TestReplayConfig_ValidateRejectsDefaultSpanAboveMaxLookback(t *testing.T) {
	c := ReplayConfig{DefaultSpan: 30 * 24 * time.Hour, MaxLookback: 7 * 24 * time.Hour}
	if err := c.validate(); err == nil {
		t.Fatal("want error when DefaultSpan > MaxLookback")
	}
}
