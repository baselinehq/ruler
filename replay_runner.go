package ruler

import (
	"context"
	"fmt"
	"strconv"
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

// emitProgressMarker writes a single sample to ProgressMetric series.
// Best-effort: failures logged, not propagated.
func (r *replayRunner) emitProgressMarker(ctx context.Context, chunkEnd time.Time) {
	if r.cfg.ProgressMetric == "" {
		return
	}
	series := prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: "__name__", Value: r.cfg.ProgressMetric},
			{Name: "rule_id", Value: strconv.FormatUint(r.rule.ID, 10)},
			{Name: "record", Value: r.rule.Record},
		},
		Samples: []prompb.Sample{{
			Value:     float64(chunkEnd.Unix()),
			Timestamp: time.Now().UnixMilli(),
		}},
	}
	if err := r.writer.Write(ctx, []prompb.TimeSeries{series}); err != nil {
		r.logger.Errorf("replay: progress marker write failed for rule %q: %v", r.rule.Record, err)
	}
}

// execChunk runs one chunk: QueryRange → toTimeSeries → Write, with retry.
func (r *replayRunner) execChunk(ctx context.Context, chunk timeRange) error {
	chunkCtx, cancel := context.WithTimeout(ctx, r.cfg.ChunkTimeout)
	defer cancel()

	var res Result
	err := retryChunk(chunkCtx, defaultRetryConfig(), func() error {
		var qErr error
		res, qErr = r.q.QueryRange(chunkCtx, r.rule.Expr, chunk.Start, chunk.End)
		return qErr
	})
	if err != nil {
		return fmt.Errorf("query chunk [%v,%v]: %w", chunk.Start, chunk.End, err)
	}

	tss := r.toTimeSeriesMatrix(res)
	if len(tss) == 0 {
		return nil
	}

	if err := retryChunk(chunkCtx, defaultRetryConfig(), func() error {
		return r.writer.Write(chunkCtx, tss)
	}); err != nil {
		return fmt.Errorf("write chunk [%v,%v]: %w", chunk.Start, chunk.End, err)
	}

	if r.coord != nil {
		r.coord.updateProgress(r.rule.ID, chunk.End.UnixMilli())
	}
	return nil
}

// planChunks splits gaps into BatchInterval-sized sub-ranges.
func planChunks(gaps []timeRange, batchInterval time.Duration) []timeRange {
	var out []timeRange
	for _, g := range gaps {
		t := g.Start
		for t.Before(g.End) {
			end := t.Add(batchInterval)
			if end.After(g.End) {
				end = g.End
			}
			out = append(out, timeRange{Start: t, End: end})
			t = end
		}
	}
	return out
}

// probeCoverage queries the output metric over [start, end) and returns the
// list of gaps that need to be backfilled. When ProbeOutput is false, returns
// a single gap covering the entire span.
func (r *replayRunner) probeCoverage(ctx context.Context, start, end time.Time, step time.Duration) ([]timeRange, error) {
	if !r.cfg.ProbeOutput {
		return []timeRange{{Start: start, End: end}}, nil
	}
	expr := "count(" + r.rule.Record + ")"
	probe, err := r.q.QueryRange(ctx, expr, start, end)
	if err != nil {
		return nil, err
	}
	merge := r.cfg.GapMergeWindow
	return findGaps(probe, merge, start, end, step), nil
}

// readProgressMarker returns the latest watermark for this rule, or zero time
// if no marker exists or feature disabled.
func (r *replayRunner) readProgressMarker(ctx context.Context) (time.Time, error) {
	if r.cfg.ProgressMetric == "" {
		return time.Time{}, nil
	}
	expr := r.cfg.ProgressMetric + `{rule_id="` + strconv.FormatUint(r.rule.ID, 10) + `"}`
	res, err := r.q.Query(ctx, expr, time.Now())
	if err != nil {
		return time.Time{}, err
	}
	var maxV float64
	for _, m := range res.Data {
		for _, v := range m.Values {
			if v > maxV {
				maxV = v
			}
		}
	}
	if maxV == 0 {
		return time.Time{}, nil
	}
	return time.Unix(int64(maxV), 0), nil
}

// waitUpstreams blocks until each upstream's done channel closes (or ctx done).
// If any upstream's outcome is not a success, returns a cascade-skip error
// naming the failing upstream.
func (r *replayRunner) waitUpstreams(ctx context.Context, upstreamIDs []uint64) error {
	for i, ch := range r.upstreams {
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
		upID := upstreamIDs[i]
		outcome := r.coord.outcome(upID)
		if !outcome.IsSuccess() {
			return fmt.Errorf("upstream rule_id=%d outcome=%s; cascading skip", upID, outcome)
		}
	}
	return nil
}
