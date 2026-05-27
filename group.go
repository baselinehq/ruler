package ruler

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/prometheus/common/model"
)

type groupRunner struct {
	mu sync.RWMutex

	id                 uint64
	checksum           string
	name               string
	file               string
	interval           time.Duration
	evalOffset         *time.Duration
	evalDelay          time.Duration
	evalAlignment      *bool
	limit              int
	concurrency        int
	params             url.Values
	headers            map[string]string
	debug              bool
	updateEntriesLimit int

	qb     QuerierBuilder
	writer SeriesWriter
	logger Logger

	rules []*RecordingRule

	doneCh    chan struct{}
	updateCh  chan *groupRunner
	wg        sync.WaitGroup
	startOnce sync.Once

	cancelMu   sync.Mutex
	evalCancel context.CancelFunc
}

type groupRunnerDeps struct {
	qb                 QuerierBuilder
	writer             SeriesWriter
	logger             Logger
	externalLabels     map[string]string
	evaluationInterval time.Duration
	evalDelay          time.Duration
	updateEntriesLimit int
}

func newGroupRunner(cfg Group, deps groupRunnerDeps) (*groupRunner, error) {
	interval := deps.evaluationInterval
	if cfg.Interval != nil && time.Duration(*cfg.Interval) > 0 {
		interval = time.Duration(*cfg.Interval)
	}

	if interval <= 0 {
		return nil, fmt.Errorf("interval must be greater than 0")
	}

	if cfg.EvalOffset != nil && time.Duration(*cfg.EvalOffset) > interval {
		return nil, fmt.Errorf("eval_offset should be smaller than interval; now eval_offset: %v, interval: %v", time.Duration(*cfg.EvalOffset), interval)
	}

	evalDelay := deps.evalDelay
	if cfg.EvalDelay != nil {
		evalDelay = time.Duration(*cfg.EvalDelay)
	}

	concurrency := cfg.Concurrency
	if concurrency < 1 {
		concurrency = 1
	}

	headers := map[string]string{}
	for _, h := range cfg.Headers {
		headers[h.Key] = h.Value
	}

	params := cfg.Params.Values()

	g := &groupRunner{
		id:                 createGroupID(cfg, interval),
		checksum:           cfg.Checksum,
		name:               cfg.Name,
		file:               cfg.File,
		interval:           interval,
		evalOffset:         durationPtr(cfg.EvalOffset),
		evalDelay:          evalDelay,
		evalAlignment:      cfg.EvalAlignment,
		limit:              intOrZero(cfg.Limit),
		concurrency:        concurrency,
		params:             params,
		headers:            headers,
		debug:              cfg.Debug,
		updateEntriesLimit: deps.updateEntriesLimit,
		qb:                 deps.qb,
		writer:             deps.writer,
		logger:             deps.logger,
		doneCh:             make(chan struct{}),
		updateCh:           make(chan *groupRunner, 1),
	}

	for i := range cfg.Rules {
		debug := g.debug
		if cfg.Rules[i].Debug != nil {
			debug = *cfg.Rules[i].Debug
		}
		labels := mergeLabelSets(deps.logger, g.name, cfg.Rules[i].Name(), deps.externalLabels, cfg.Labels)
		labels = mergeLabelSets(deps.logger, g.name, cfg.Rules[i].Name(), labels, cfg.Rules[i].Labels)
		g.rules = append(g.rules, newRecordingRule(deps.qb, g, cfg.Rules[i], labels, debug))
	}

	return g, nil
}

func (g *groupRunner) Start(ctx context.Context, maxStartDelay time.Duration) {
	g.startOnce.Do(func() {
		g.wg.Add(1)
		go g.run(ctx, maxStartDelay)
	})
}

func (g *groupRunner) run(ctx context.Context, maxStartDelay time.Duration) {
	defer g.wg.Done()

	evalTS := time.Now()
	delay := g.delayBeforeStart(evalTS, maxStartDelay)
	if delay > 0 {
		g.logger.Infof("group %q will start in %v", g.name, delay)
		timer := time.NewTimer(delay)
		for {
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-g.doneCh:
				timer.Stop()
				return
			case ng := <-g.updateCh:
				g.applyUpdate(ng)
			case <-timer.C:
				goto startEval
			}
		}
	}

startEval:
	evalTS = evalTS.Add(delay)
	evalCtx, cancel := context.WithCancel(ctx)
	g.cancelMu.Lock()
	g.evalCancel = cancel
	g.cancelMu.Unlock()
	g.eval(evalCtx, evalTS)

	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-g.doneCh:
			return
		case ng := <-g.updateCh:
			g.cancelMu.Lock()
			if g.evalCancel != nil {
				g.evalCancel()
			}
			evalCtx, cancel = context.WithCancel(ctx)
			g.evalCancel = cancel
			g.cancelMu.Unlock()
			g.applyUpdate(ng)
		case <-ticker.C:
			offset := time.Now().Round(0).Sub(evalTS.Round(0))
			missed := (offset / g.interval) - 1
			if missed < 0 {
				missed = 0
				evalTS = time.Now()
			}
			evalTS = evalTS.Add((missed + 1) * g.interval)
			g.eval(evalCtx, evalTS)
		}
	}
}

func (g *groupRunner) eval(ctx context.Context, ts time.Time) {
	g.mu.RLock()
	// Safe to read directly,rules are replaced atomically, never mutated in-place
	rules := g.rules
	limit := g.limit
	concurrency := g.concurrency
	queryTS := g.queryTimestamp(ts)
	g.mu.RUnlock()

	if len(rules) == 0 {
		return
	}

	errs := execConcurrently(rules, concurrency, func(rule *RecordingRule) error {
		tss, err := rule.exec(ctx, queryTS, limit)
		if err != nil {
			return err
		}

		if len(tss) == 0 {
			return nil
		}

		if g.writer == nil {
			return ErrNoWriter
		}

		if err := g.writer.Write(ctx, tss); err != nil {
			return fmt.Errorf("remote write failure: %w", err)
		}
		return nil
	})

	for err := range errs {
		if err != nil {
			g.logger.Errorf("group %q: %s", g.name, err)
		}
	}
}

func (g *groupRunner) Stop() {
	select {
	case <-g.doneCh:
		return
	default:
		close(g.doneCh)
	}
	g.cancelMu.Lock()
	if g.evalCancel != nil {
		g.evalCancel()
	}
	g.cancelMu.Unlock()
	g.wg.Wait()
}

func (g *groupRunner) InterruptEval() {
	g.cancelMu.Lock()
	if g.evalCancel != nil {
		g.evalCancel()
	}
	g.cancelMu.Unlock()
}

func (g *groupRunner) UpdateWith(newGroup *groupRunner) error {
	// Non-blocking send with latest wins semantics to prevent deadlock
	// when eval() is running and can't receive from updateCh.
	select {
	case g.updateCh <- newGroup:
		return nil
	case <-g.doneCh:
		return fmt.Errorf("group %q is stopped", g.name)
	default:
		// Channel is full (eval is running), drain old update and replace with new one
		select {
		case <-g.updateCh:
		case <-g.doneCh:
			return fmt.Errorf("group %q is stopped", g.name)
		default:
		}
		// Try to send again (should succeed now)
		select {
		case g.updateCh <- newGroup:
			return nil
		case <-g.doneCh:
			return fmt.Errorf("group %q is stopped", g.name)
		}
	}
}

func (g *groupRunner) applyUpdate(newGroup *groupRunner) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Replace rule pointers instead of mutating them to avoid race conditions
	// with in-flight eval() goroutines. Old evals will complete with old rules,
	// new evals will use new rules.
	//
	// BEHAVIORAL NOTE: This creates new RecordingRule objects, which resets
	// per-rule state:
	//   - lastEvaluation: No stale markers emitted for series that existed
	//     before the update but don't match after.
	//   - ruleState: Evaluation history (success/failure, duration, samples)
	//     is reset.
	// This is an acceptable tradeoff for race-free updates. Config changes are
	// typically infrequent, and old series will expire naturally.
	g.rules = newGroup.rules
	g.checksum = newGroup.checksum
	g.concurrency = newGroup.concurrency
	g.limit = newGroup.limit
	g.params = newGroup.params
	g.headers = newGroup.headers
	g.debug = newGroup.debug
	g.evalDelay = newGroup.evalDelay
	g.evalAlignment = newGroup.evalAlignment
}

// queryTimestamp computes the actual query timestamp to use for rule evaluation.
// It applies eval_offset (if set), eval_delay (lookback), and optional alignment.
func (g *groupRunner) queryTimestamp(timestamp time.Time) time.Time {
	// If eval_offset is set, timestamp is already aligned; skip eval_delay.
	if g.evalOffset != nil {
		return timestamp
	}

	// Normal path apply delay and optional alignment.
	timestamp = timestamp.Add(-g.evalDelay)
	if g.evalAlignment == nil || *g.evalAlignment {
		return timestamp.Truncate(g.interval)
	}

	return timestamp
}

func (g *groupRunner) delayBeforeStart(ts time.Time, maxDelay time.Duration) time.Duration {
	if g.evalOffset != nil {
		currentOffsetPoint := ts.Truncate(g.interval).Add(*g.evalOffset)
		if currentOffsetPoint.Before(ts) {
			return currentOffsetPoint.Add(g.interval).Sub(ts)
		}
		return currentOffsetPoint.Sub(ts)
	}

	interval := min(g.interval, maxDelay)

	if interval <= 0 {
		return 0
	}
	// Use uint64 math for better precision and distribution
	// This avoids float64 precision loss for large group IDs
	randSleep := time.Duration((g.id % uint64(interval.Nanoseconds())))
	sleepOffset := time.Duration(ts.UnixNano() % interval.Nanoseconds())
	if randSleep < sleepOffset {
		randSleep += interval
	}

	return randSleep - sleepOffset
}

func execConcurrently(rules []*RecordingRule, concurrency int, fn func(rule *RecordingRule) error) chan error {
	res := make(chan error, len(rules))
	if concurrency <= 1 {
		for _, rule := range rules {
			res <- fn(rule)
		}
		close(res)
		return res
	}

	sem := make(chan struct{}, concurrency)
	go func() {
		var wg sync.WaitGroup
		for i := range rules {
			rule := rules[i]
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				res <- fn(rule)
				<-sem
			}()
		}
		wg.Wait()
		close(res)
	}()

	return res
}

func createGroupID(cfg Group, interval time.Duration) uint64 {
	h := fnv64a{}
	h.WriteString(cfg.File)
	h.WriteString("\xff")
	h.WriteString(cfg.Name)
	h.WriteString(cfg.Type)
	h.WriteString(interval.String())
	if cfg.EvalOffset != nil {
		h.WriteString(cfg.EvalOffset.String())
	}

	return h.Sum64()
}

func durationPtr(d *model.Duration) *time.Duration {
	if d == nil {
		return nil
	}
	v := time.Duration(*d)

	return &v
}

func intOrZero(v *int) int {
	if v == nil {
		return 0
	}

	return *v
}
