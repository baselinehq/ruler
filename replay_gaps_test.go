package ruler

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

func tr(start, end time.Time) timeRange { return timeRange{Start: start, End: end} }

func TestFindGaps_FullyCovered(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	start := now.Add(-1 * time.Hour)
	step := 5 * time.Minute
	ts := make([]int64, 0, 12)
	vals := make([]float64, 0, 12)
	for i := 0; i < 12; i++ {
		ts = append(ts, start.Add(time.Duration(i)*step).UnixMilli())
		vals = append(vals, 1)
	}
	probe := Result{Data: []Metric{{Labels: []prompb.Label{}, Timestamps: ts, Values: vals}}}
	got := findGaps(probe, 0, start, now, step)
	if len(got) != 0 {
		t.Errorf("got %v, want []", got)
	}
}

func TestFindGaps_FullyEmpty(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	start := now.Add(-1 * time.Hour)
	step := 5 * time.Minute
	got := findGaps(Result{}, 0, start, now, step)
	if len(got) != 1 {
		t.Fatalf("got %v, want 1 gap", got)
	}
	if !got[0].Start.Equal(start) || !got[0].End.Equal(now) {
		t.Errorf("gap = %v, want [%v, %v]", got[0], start, now)
	}
}

func TestFindGaps_MiddleHole(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	start := now.Add(-1 * time.Hour)
	step := 5 * time.Minute
	ts := []int64{
		start.UnixMilli(),
		start.Add(5 * time.Minute).UnixMilli(),
		start.Add(30 * time.Minute).UnixMilli(),
		start.Add(35 * time.Minute).UnixMilli(),
		start.Add(55 * time.Minute).UnixMilli(),
	}
	vals := []float64{1, 1, 1, 1, 1}
	probe := Result{Data: []Metric{{Timestamps: ts, Values: vals}}}
	got := findGaps(probe, 0, start, now, step)
	if len(got) < 1 {
		t.Fatalf("got %v, want at least one gap", got)
	}
}

func TestMergeGaps_BelowWindowCoalesce(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0)
	gaps := []timeRange{
		tr(t0, t0.Add(10*time.Minute)),
		tr(t0.Add(15*time.Minute), t0.Add(25*time.Minute)),
	}
	got := mergeGaps(gaps, 10*time.Minute)
	if len(got) != 1 {
		t.Fatalf("got %v, want 1 merged gap", got)
	}
	if !got[0].Start.Equal(t0) || !got[0].End.Equal(t0.Add(25*time.Minute)) {
		t.Errorf("merged = %v", got[0])
	}
}

func TestMergeGaps_AboveWindowPreserved(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0)
	gaps := []timeRange{
		tr(t0, t0.Add(10*time.Minute)),
		tr(t0.Add(40*time.Minute), t0.Add(50*time.Minute)),
	}
	got := mergeGaps(gaps, 10*time.Minute)
	if len(got) != 2 {
		t.Errorf("got %v, want 2 preserved gaps", got)
	}
}
