package ruler

import (
	"context"
	"fmt"
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

// validateSources verifies that every external metric referenced by the rule
// expression exists in inv. Recorded-rule refs (those in records) are skipped
// because their existence is the responsibility of the upstream replayRunner.
func (r *replayRunner) validateSources(inv *metricInventory, records map[string]uint64) error {
	selectors, err := extractSelectors(r.rule.Expr)
	if err != nil {
		return err
	}
	var missing []string
	for _, name := range selectors {
		if _, isRecorded := records[name]; isRecorded {
			continue
		}
		if !inv.Has(name) {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("source metrics not found in TSDB: %v", missing)
	}
	return nil
}

// Touch context import - used by later phases of the runner.
var _ = context.Background
