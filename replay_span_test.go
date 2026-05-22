package ruler

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
)

func dur(d time.Duration) *model.Duration { v := model.Duration(d); return &v }

func TestResolveSpan_GroupOverridesManager(t *testing.T) {
	mgr := ReplayConfig{DefaultSpan: 7 * 24 * time.Hour}
	grp := &GroupReplayConfig{Span: dur(30 * 24 * time.Hour)}
	span, _, err := resolveSpan(mgr, grp, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if span != 30*24*time.Hour {
		t.Errorf("span = %v, want 30d", span)
	}
}

func TestResolveSpan_ManagerDefaultUsed(t *testing.T) {
	mgr := ReplayConfig{DefaultSpan: 7 * 24 * time.Hour}
	span, _, err := resolveSpan(mgr, nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if span != 7*24*time.Hour {
		t.Errorf("span = %v, want 7d", span)
	}
}

func TestResolveSpan_MaxLookbackClamps(t *testing.T) {
	mgr := ReplayConfig{DefaultSpan: 30 * 24 * time.Hour, MaxLookback: 14 * 24 * time.Hour}
	span, _, err := resolveSpan(mgr, nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if span != 14*24*time.Hour {
		t.Errorf("span = %v, want 14d", span)
	}
}

func TestResolveSpan_GroupMaxLookbackClamps(t *testing.T) {
	mgr := ReplayConfig{DefaultSpan: 30 * 24 * time.Hour}
	grp := &GroupReplayConfig{MaxLookback: dur(7 * 24 * time.Hour)}
	span, _, err := resolveSpan(mgr, grp, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if span != 7*24*time.Hour {
		t.Errorf("span = %v, want 7d", span)
	}
}

func TestResolveSpan_ZeroReturnsErrNoSpan(t *testing.T) {
	mgr := ReplayConfig{}
	_, _, err := resolveSpan(mgr, nil, 0, 0)
	if err != ErrReplayNoSpan {
		t.Errorf("err = %v, want ErrReplayNoSpan", err)
	}
}

func TestResolveSpan_RetentionClamps(t *testing.T) {
	mgr := ReplayConfig{DefaultSpan: 30 * 24 * time.Hour}
	span, clamped, err := resolveSpan(mgr, nil, 5*time.Minute, 14*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if span != 14*24*time.Hour {
		t.Errorf("span = %v, want 14d (clamped)", span)
	}
	if !clamped {
		t.Errorf("clamped = false, want true")
	}
}

func TestResolveSpan_RetentionBelowMinDepth(t *testing.T) {
	mgr := ReplayConfig{DefaultSpan: 30 * 24 * time.Hour}
	_, _, err := resolveSpan(mgr, nil, 7*24*time.Hour, 3*24*time.Hour)
	if err != ErrReplayRetentionBelowMinDepth {
		t.Errorf("err = %v, want ErrReplayRetentionBelowMinDepth", err)
	}
}
