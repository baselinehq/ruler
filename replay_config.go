package ruler

import (
	"fmt"
	"time"

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
