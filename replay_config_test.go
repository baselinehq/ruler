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
	if !contains(out, []byte("span: 30d")) {
		t.Errorf("round trip lost span: %s", out)
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

func contains(haystack, needle []byte) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		match := true
		for j := range needle {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
