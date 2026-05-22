package ruler

import (
	"errors"
	"time"
)

var (
	ErrReplayNoSpan                 = errors.New("replay: no span configured (set DefaultSpan or group.replay.span)")
	ErrReplayRetentionBelowMinDepth = errors.New("replay: source retention shorter than rule's minimum source depth")
)

// resolveSpan applies precedence: group > manager > MaxLookback ceiling > retention.
// minDepth is the maximum range selector in the rule expr (informational; only
// used to compare against retention).
// retention is the detected source retention (0 = unknown / detection disabled).
// Returns: resolved span, true if retention clamped, error if span unusable.
func resolveSpan(mgr ReplayConfig, grp *GroupReplayConfig, minDepth, retention time.Duration) (time.Duration, bool, error) {
	span := mgr.DefaultSpan
	if grp != nil && grp.Span != nil {
		span = time.Duration(*grp.Span)
	}
	if span <= 0 {
		return 0, false, ErrReplayNoSpan
	}

	maxLookback := mgr.MaxLookback
	if grp != nil && grp.MaxLookback != nil {
		maxLookback = time.Duration(*grp.MaxLookback)
	}
	if maxLookback > 0 && span > maxLookback {
		span = maxLookback
	}

	clamped := false
	if retention > 0 {
		if retention < minDepth {
			return 0, false, ErrReplayRetentionBelowMinDepth
		}
		if retention < span {
			span = retention
			clamped = true
		}
	}

	return span, clamped, nil
}
