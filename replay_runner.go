package ruler

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

// replayRunner backfills a single recording rule over a configured historical span.
// One goroutine per rule per process lifetime.
type replayRunner struct {
	rule       Rule
	groupName  string
	groupCfg   *GroupReplayConfig
	cfg        ReplayConfig
	ruleLabels []prompb.Label
	minDepth   time.Duration

	q      Querier
	writer SeriesWriter
	coord  *replayCoordinator
	logger Logger

	upstreams []chan struct{} // closed when upstream finishes
	done      chan struct{}   // closed by setOutcome
}

// toTimeSeriesMatrix converts a QueryRange result into prompb.TimeSeries,
// applying the rule's record name + merged labels (same semantics as
// RecordingRule.toTimeSeries but iterating the matrix and preserving
// chunk timestamps).
func (r *replayRunner) toTimeSeriesMatrix(res Result) []prompb.TimeSeries {
	out := make([]prompb.TimeSeries, 0, len(res.Data))
	for _, m := range res.Data {
		labels := make([]prompb.Label, 0, len(m.Labels)+1+len(r.ruleLabels))
		labels = append(labels, m.Labels...)
		labels = applyName(labels, r.rule.Record)
		sortLabels(labels)
		labels = applyRuleLabels(labels, r.ruleLabels)

		samples := make([]prompb.Sample, len(m.Timestamps))
		for i := range m.Timestamps {
			samples[i] = prompb.Sample{
				Value:     m.Values[i],
				Timestamp: m.Timestamps[i],
			}
		}
		out = append(out, prompb.TimeSeries{Labels: labels, Samples: samples})
	}
	return out
}

// Touch context import - used by later phases of the runner.
var _ = context.Background
