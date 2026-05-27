// Portions of this file are derived from VictoriaMetrics vmalert replay code:
//   - https://github.com/VictoriaMetrics/VictoriaMetrics/blob/v1.130.0/app/vmalert/rule/group.go
//   - https://github.com/VictoriaMetrics/VictoriaMetrics/blob/v1.130.0/app/vmalert/rule/rule.go
//   - https://github.com/VictoriaMetrics/VictoriaMetrics/blob/v1.130.0/app/vmalert/rule/recording.go
//
// Copyright 2019-2026 VictoriaMetrics, Inc.
// Licensed under the Apache License, Version 2.0.
//
// Modifications: adapted to github.com/baselinehq/ruler native rule,
// datasource, and remote-write interfaces; errors are returned to callers
// instead of terminating the process.

package ruler

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"golang.org/x/sync/errgroup"
)

// replay adapts this package's config, querier, and writer
// interfaces to a vmalert-style replay implementation.
type replay struct {
	opts ReplayOptions
	groupRunnerDeps
}

// ReplayOptions configures a single historical backfill run.
type ReplayOptions struct {
	// TimeFrom is the inclusive replay start timestamp.
	TimeFrom time.Time
	// TimeTo is the replay end timestamp. If unset or too recent, it is clamped
	// behind live evaluation to avoid racing current rule writes.
	TimeTo time.Time

	// MaxDatapointsPerQuery controls replay chunk width as
	// group interval * max datapoints.
	MaxDatapointsPerQuery int
	// RuleRetryAttempts is the number of attempts for each rule chunk.
	RuleRetryAttempts int
	// RulesDelay sleeps between rules in a group so chained recording rules can
	// read data written by earlier rules.
	RulesDelay time.Duration
}

func newReplay(opts ReplayOptions, deps groupRunnerDeps) (*replay, error) {
	if deps.qb == nil {
		return nil, fmt.Errorf("querier builder is required")
	}
	if deps.writer == nil {
		return nil, ErrNoWriter
	}

	deps.updateEntriesLimit = 1

	if opts.TimeFrom.IsZero() {
		return nil, fmt.Errorf("TimeFrom must be set")
	}
	if !opts.TimeTo.IsZero() && !opts.TimeTo.After(opts.TimeFrom) {
		return nil, fmt.Errorf("TimeTo (%v) must be after TimeFrom (%v)", opts.TimeTo, opts.TimeFrom)
	}
	if opts.MaxDatapointsPerQuery < 0 {
		return nil, fmt.Errorf("MaxDatapointsPerQuery must be >= 0")
	}
	if opts.RuleRetryAttempts < 0 {
		return nil, fmt.Errorf("RuleRetryAttempts must be >= 0")
	}
	if opts.RulesDelay < 0 {
		return nil, fmt.Errorf("RulesDelay must be >= 0")
	}
	if opts.MaxDatapointsPerQuery == 0 {
		opts.MaxDatapointsPerQuery = 1000
	}
	if opts.RuleRetryAttempts == 0 {
		opts.RuleRetryAttempts = 5
	}

	return &replay{
		opts:            opts,
		groupRunnerDeps: deps,
	}, nil
}

func (r *replay) run(ctx context.Context, cfg Config) error {
	groups, err := r.buildGroups(cfg)
	if err != nil {
		return err
	}
	for _, group := range groups {
		rows, err := r.replayGroup(ctx, group.runner, group.start, group.end)
		if err != nil {
			return fmt.Errorf("group %q: %w", group.runner.name, err)
		}
		r.logger.Infof("replay: group=%q finished, generated %d samples", group.runner.name, rows)
	}

	return nil
}

type replayGroup struct {
	runner     *groupRunner
	start, end time.Time
}

func (r *replay) buildGroups(cfg Config) ([]replayGroup, error) {
	groups := make([]replayGroup, 0, len(cfg.Groups))
	seen := make(map[uint64]string, len(cfg.Groups))
	for _, group := range cfg.Groups {
		rg, err := newGroupRunner(group, r.groupRunnerDeps)
		if err != nil {
			return nil, fmt.Errorf("group %q: %w", group.Name, err)
		}
		if existing, ok := seen[rg.id]; ok {
			return nil, fmt.Errorf("duplicate group identity: %q and %q hash to same id", existing, rg.name)
		}
		seen[rg.id] = rg.name

		start, end, err := r.replayRange(rg)
		if err != nil {
			return nil, fmt.Errorf("group %q: %w", group.Name, err)
		}
		groups = append(groups, replayGroup{runner: rg, start: start, end: end})
	}

	return groups, nil
}

func (r *replay) replayRange(group *groupRunner) (time.Time, time.Time, error) {
	delay := group.evalDelay
	if group.evalOffset != nil {
		delay = 0
	}

	safeEnd := time.Now().Add(-(2*group.interval + delay))
	end := r.opts.TimeTo
	if end.IsZero() || end.After(safeEnd) {
		end = safeEnd
	}

	start := r.opts.TimeFrom
	if !end.After(start) {
		return time.Time{}, time.Time{}, fmt.Errorf("time range [%v,%v] is empty after clamping end to %v", start, r.opts.TimeTo, end)
	}

	return start, end, nil
}

func (r *replay) replayGroup(ctx context.Context, group *groupRunner, start, end time.Time) (int, error) {
	if int64(r.opts.MaxDatapointsPerQuery) > math.MaxInt64/int64(group.interval) {
		return 0, fmt.Errorf("replay chunk step overflows duration")
	}
	step := group.interval * time.Duration(r.opts.MaxDatapointsPerQuery)
	ri := replayRangeIterator{start: start, end: end, step: step}

	concurrency := group.concurrency
	if concurrency > 1 && r.opts.RulesDelay > 0 {
		r.logger.Warnf("replay: group=%q concurrency=%d ignored because rulesDelay=%v", group.name, concurrency, r.opts.RulesDelay)
		concurrency = 1
	}

	if concurrency == 1 {
		return r.replayRulesSequentially(ctx, group.rules, ri, group.limit)
	}

	return r.replayRulesConcurrently(ctx, group.rules, ri, group.limit, concurrency)
}

func (r *replay) replayRulesSequentially(ctx context.Context, rules []*RecordingRule, ri replayRangeIterator, limit int) (int, error) {
	var total int
	for i, rule := range rules {
		n, err := r.replayRuleRange(ctx, rule, ri, limit)
		total += n
		if err != nil {
			return total, err
		}

		if i < len(rules)-1 {
			if err := sleepContext(ctx, r.opts.RulesDelay); err != nil {
				return total, err
			}
		}
	}

	return total, nil
}

func (r *replay) replayRulesConcurrently(ctx context.Context, rules []*RecordingRule, ri replayRangeIterator, limit, concurrency int) (int, error) {
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(concurrency)

	var total atomic.Int64
	for _, rule := range rules {
		group.Go(func() error {
			n, err := r.replayRuleRange(ctx, rule, ri, limit)
			total.Add(int64(n))
			return err
		})
	}

	return int(total.Load()), group.Wait()
}

func (r *replay) replayRuleRange(ctx context.Context, rule *RecordingRule, ri replayRangeIterator, limit int) (int, error) {
	var total int
	for ri.next() {
		n, err := r.replayRule(ctx, rule, ri.s, ri.e, limit)
		total += n
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

func (r *replay) replayRule(ctx context.Context, rule *RecordingRule, start, end time.Time, limit int) (int, error) {
	var tss []prompb.TimeSeries
	var err error
	for attempt := 1; attempt <= r.opts.RuleRetryAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		tss, err = rule.execRange(ctx, start, end, limit)
		if err == nil {
			break
		}

		r.logger.Errorf("replay: attempt %d to execute rule %q failed: %s", attempt, rule.name, err)
		if attempt < r.opts.RuleRetryAttempts {
			if sleepErr := sleepContext(ctx, time.Second); sleepErr != nil {
				return 0, sleepErr
			}
		}
	}
	if err != nil {
		return 0, err
	}
	if len(tss) == 0 {
		return 0, nil
	}
	if err := r.writer.Write(ctx, tss); err != nil {
		return 0, fmt.Errorf("remote write failure: %w", err)
	}

	var samples int
	for _, ts := range tss {
		samples += len(ts.Samples)
	}

	return samples, nil
}

// execRange executes recording rule on the given time range similarly to exec.
// It doesn't update rule state and is meant only for backfilling.
func (r *RecordingRule) execRange(ctx context.Context, start, end time.Time, limit int) ([]prompb.TimeSeries, error) {
	res, err := r.q.QueryRange(ctx, r.expr, start, end)
	if err != nil {
		return nil, err
	}
	if limit > 0 && len(res.Data) > limit {
		return nil, fmt.Errorf("exec exceeded limit of %d with %d series", limit, len(res.Data))
	}

	tss, _, err := r.toTimeSeriesSet(res.Data)
	if err != nil {
		return nil, fmt.Errorf("recording rule %q: %w", r.name, err)
	}
	
	return tss, nil
}

type replayRangeIterator struct {
	step       time.Duration
	start, end time.Time

	iter int
	s, e time.Time
}

func (ri *replayRangeIterator) next() bool {
	ri.s = ri.start.Add(ri.step * time.Duration(ri.iter))
	if !ri.end.After(ri.s) {
		return false
	}

	ri.e = ri.s.Add(ri.step)
	if ri.e.After(ri.end) {
		ri.e = ri.end
	}

	ri.iter++

	return true
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
