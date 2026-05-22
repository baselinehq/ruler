package ruler

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
)

// GroupReplayConfig is the per-group YAML override for replay behavior.
// All fields are pointers so we can distinguish "unset" from "explicitly zero/false".
type GroupReplayConfig struct {
	Enabled     *bool           `yaml:"enabled,omitempty"`
	Span        *model.Duration `yaml:"span,omitempty"`
	MaxLookback *model.Duration `yaml:"max_lookback,omitempty"`
}

// Validate checks per-group replay fields.
func (g *GroupReplayConfig) Validate() error {
	if g == nil {
		return nil
	}
	if g.Span != nil && time.Duration(*g.Span) < 0 {
		return fmt.Errorf("replay.span shouldn't be lower than 0")
	}
	if g.MaxLookback != nil && time.Duration(*g.MaxLookback) < 0 {
		return fmt.Errorf("replay.max_lookback shouldn't be lower than 0")
	}
	if g.Span != nil && g.MaxLookback != nil && time.Duration(*g.MaxLookback) < time.Duration(*g.Span) {
		return fmt.Errorf("replay.max_lookback (%v) must be >= replay.span (%v)", time.Duration(*g.MaxLookback), time.Duration(*g.Span))
	}
	return nil
}

// ReplayConfig configures historical backfill on Manager.Apply.
// nil or Enabled=false disables the feature entirely.
type ReplayConfig struct {
	Enabled bool

	DefaultSpan time.Duration
	MaxLookback time.Duration

	BatchInterval    time.Duration
	Concurrency      int
	RulesConcurrency int
	ChunkTimeout     time.Duration

	ProbeOutput    bool
	GapMergeWindow time.Duration

	ProgressMetric string

	DetectSourceRetention bool

	Registerer prometheus.Registerer
}

// applyDefaults fills zero-valued fields with documented defaults.
// Returns a new ReplayConfig; receiver not mutated.
func (c ReplayConfig) applyDefaults() ReplayConfig {
	if c.BatchInterval <= 0 {
		c.BatchInterval = 6 * time.Hour
	}
	if c.Concurrency < 1 {
		c.Concurrency = 2
	}
	if c.RulesConcurrency < 1 {
		c.RulesConcurrency = 4
	}
	if c.ChunkTimeout <= 0 {
		c.ChunkTimeout = 5 * time.Minute
	}
	if c.GapMergeWindow <= 0 {
		c.GapMergeWindow = 2 * c.BatchInterval
	}
	return c
}

// validate checks ReplayConfig-level invariants.
func (c ReplayConfig) validate() error {
	if c.DefaultSpan < 0 {
		return fmt.Errorf("DefaultSpan must be >= 0")
	}
	if c.MaxLookback < 0 {
		return fmt.Errorf("MaxLookback must be >= 0")
	}
	if c.MaxLookback > 0 && c.DefaultSpan > c.MaxLookback {
		return fmt.Errorf("DefaultSpan (%v) must be <= MaxLookback (%v)", c.DefaultSpan, c.MaxLookback)
	}
	if c.ProgressMetric != "" && !model.IsValidLegacyMetricName(c.ProgressMetric) {
		return fmt.Errorf("ProgressMetric %q is not a valid metric name", c.ProgressMetric)
	}
	return nil
}
