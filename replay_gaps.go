package ruler

import (
	"time"
)

type timeRange struct {
	Start time.Time
	End   time.Time
}

// findGaps scans the probe matrix and returns the contiguous unpopulated
// sub-ranges within [start, end), stepped by step. A timestamp is treated as
// covered if any series in the probe has a sample with value > 0 at that tick.
// After detection, adjacent gaps closer than mergeWindow are merged.
func findGaps(probe Result, mergeWindow time.Duration, start, end time.Time, step time.Duration) []timeRange {
	covered := map[int64]bool{}
	for _, m := range probe.Data {
		for i, ts := range m.Timestamps {
			if m.Values[i] > 0 {
				covered[ts] = true
			}
		}
	}

	var gaps []timeRange
	var curStart *time.Time
	for t := start; t.Before(end); t = t.Add(step) {
		if !covered[t.UnixMilli()] {
			if curStart == nil {
				tt := t
				curStart = &tt
			}
		} else if curStart != nil {
			gaps = append(gaps, timeRange{Start: *curStart, End: t})
			curStart = nil
		}
	}
	if curStart != nil {
		gaps = append(gaps, timeRange{Start: *curStart, End: end})
	}

	return mergeGaps(gaps, mergeWindow)
}

// mergeGaps coalesces gaps separated by less than mergeWindow. Input must be
// sorted by Start.
func mergeGaps(gaps []timeRange, mergeWindow time.Duration) []timeRange {
	if len(gaps) <= 1 || mergeWindow <= 0 {
		return gaps
	}
	out := make([]timeRange, 0, len(gaps))
	cur := gaps[0]
	for i := 1; i < len(gaps); i++ {
		if gaps[i].Start.Sub(cur.End) <= mergeWindow {
			if gaps[i].End.After(cur.End) {
				cur.End = gaps[i].End
			}
		} else {
			out = append(out, cur)
			cur = gaps[i]
		}
	}
	out = append(out, cur)
	return out
}
