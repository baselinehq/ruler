package ruler

import (
	"context"
	"errors"
	"sync"
)

// replayCoordinator owns the lifecycle of replay runners and maintains
// per-process state (probed set, outcomes, in-memory progress).
type replayCoordinator struct {
	cfg            ReplayConfig
	qb             QuerierBuilder
	writer         SeriesWriter
	logger         Logger
	externalLabels map[string]string

	httpClient *HTTPClient

	mu        sync.Mutex
	probed    map[uint64]struct{}
	outcomes  map[uint64]ReplayOutcome
	doneChans map[uint64]chan struct{}
	progress  map[uint64]int64 // ruleID -> unix-millis of last completed chunk end
	inv       *metricInventory

	rulesSem chan struct{}
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc

	metrics *replayMetrics
}

// newReplayCoordinator validates cfg, applies defaults, and constructs the coordinator.
// If cfg.Enabled is false this returns nil to signal "no-op".
func newReplayCoordinator(parent context.Context, cfg ReplayConfig, qb QuerierBuilder, writer SeriesWriter, logger Logger, externalLabels map[string]string) (*replayCoordinator, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	cfg = cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	httpClient, _ := qb.(*HTTPClient) // optional; nil if caller uses a different builder
	ctx, cancel := context.WithCancel(parent)
	return &replayCoordinator{
		cfg:            cfg,
		qb:             qb,
		writer:         writer,
		logger:         ensureLogger(logger),
		externalLabels: externalLabels,
		httpClient:     httpClient,
		probed:         map[uint64]struct{}{},
		outcomes:       map[uint64]ReplayOutcome{},
		doneChans:      map[uint64]chan struct{}{},
		progress:       map[uint64]int64{},
		rulesSem:       make(chan struct{}, cfg.RulesConcurrency),
		ctx:            ctx,
		cancel:         cancel,
		metrics:        newReplayMetrics(cfg.Registerer),
	}, nil
}

// Stop cancels the coordinator context and waits for in-flight runners.
func (c *replayCoordinator) Stop() {
	if c == nil {
		return
	}
	c.cancel()
	c.wg.Wait()
}

// applyAsync runs OnApply on a tracked goroutine so Stop waits for it.
func (c *replayCoordinator) applyAsync(parent context.Context, cfg Config) {
	if c == nil {
		return
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.OnApply(parent, cfg)
	}()
}

// setOutcome stores terminal outcome + closes the rule's done channel (idempotent).
// Ensures the channel exists even when no downstream has called doneCh yet so a
// later cascade-skip cannot block forever on a freshly-created (unclosed) channel.
func (c *replayCoordinator) setOutcome(ruleID uint64, outcome ReplayOutcome) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.outcomes[ruleID] = outcome
	ch, ok := c.doneChans[ruleID]
	if !ok {
		ch = make(chan struct{})
		c.doneChans[ruleID] = ch
	}
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// outcome reads the current outcome for ruleID.
func (c *replayCoordinator) outcome(ruleID uint64) ReplayOutcome {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.outcomes[ruleID]
}

// doneCh returns (or lazily creates) the done channel for ruleID.
func (c *replayCoordinator) doneCh(ruleID uint64) chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ch, ok := c.doneChans[ruleID]; ok {
		return ch
	}
	ch := make(chan struct{})
	c.doneChans[ruleID] = ch
	return ch
}

// markProbed returns true if this rule is newly probed (caller should spawn).
func (c *replayCoordinator) markProbed(ruleID uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.probed[ruleID]; ok {
		return false
	}
	c.probed[ruleID] = struct{}{}
	return true
}

// updateProgress records the latest completed chunk end timestamp.
func (c *replayCoordinator) updateProgress(ruleID uint64, chunkEnd int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if chunkEnd > c.progress[ruleID] {
		c.progress[ruleID] = chunkEnd
	}
}

// OnApply is called from Manager.Apply after the group diff. Non-blocking.
func (c *replayCoordinator) OnApply(_ context.Context, cfg Config) {
	if c == nil {
		return
	}

	// 1. Build inventory once per process.
	if c.inv == nil && c.httpClient != nil {
		inv, err := fetchInventory(c.ctx, c.httpClient)
		if err != nil {
			c.logger.Errorf("replay: inventory fetch failed, skipping run: %v", err)
			return
		}
		c.inv = inv
	}

	// 2. Build dep graph + handle cycles.
	graph, err := buildDepGraph(cfg)
	if err != nil && !errors.Is(err, ErrReplayCycle) {
		c.logger.Errorf("replay: build dep graph failed: %v", err)
		return
	}
	if graph == nil {
		return
	}
	for _, id := range graph.cycle {
		c.logger.Errorf("replay: cycle member rule_id=%d, skipping", id)
		c.setOutcome(id, OutcomeCycle)
	}

	// 3. Map record names + index rules by ID.
	records := map[string]uint64{}
	rulesByID := map[uint64]Rule{}
	groupByRule := map[uint64]Group{}
	for _, grp := range cfg.Groups {
		for _, r := range grp.Rules {
			records[r.Record] = r.ID
			rulesByID[r.ID] = r
			groupByRule[r.ID] = grp
		}
	}

	// 4. Spawn runners in topo order.
	for _, id := range graph.order {
		if !c.markProbed(id) {
			continue
		}
		grp := groupByRule[id]
		if grp.Replay != nil && grp.Replay.Enabled != nil && !*grp.Replay.Enabled {
			c.setOutcome(id, OutcomeSkippedDisabled)
			continue
		}
		rule := rulesByID[id]
		minDepth, _ := maxRangeSelector(rule.Expr)

		// Pre-create done chan + collect upstream chans.
		_ = c.doneCh(id)
		var upChs []chan struct{}
		for _, upID := range graph.nodes[id].upstreams {
			upChs = append(upChs, c.doneCh(upID))
		}

		runner := &replayRunner{
			rule:      rule,
			groupName: grp.Name,
			groupCfg:  grp.Replay,
			cfg:       c.cfg,
			minDepth:  minDepth,
			q:         c.qb.Build(QueryParams{EvaluationInterval: c.cfg.BatchInterval}),
			writer:    c.writer,
			coord:     c,
			logger:    c.logger,
			upstreams: upChs,
			done:      c.doneCh(id),
		}
		labels := mergeLabelSets(c.logger, grp.Name, rule.Name(), c.externalLabels, grp.Labels)
		labels = mergeLabelSets(c.logger, grp.Name, rule.Name(), labels, rule.Labels)
		runner.ruleLabels = prepareRuleLabels(labels)

		if c.inv != nil {
			if err := runner.validateSources(c.inv, records); err != nil {
				c.logger.Errorf("replay: validate sources rule=%q: %v", rule.Record, err)
				c.setOutcome(id, OutcomeSkippedNoSource)
				continue
			}
		}

		select {
		case c.rulesSem <- struct{}{}:
		case <-c.ctx.Done():
			return
		}
		c.wg.Add(1)
		go func(rnr *replayRunner, upIDs []uint64) {
			defer c.wg.Done()
			defer func() { <-c.rulesSem }()
			if err := rnr.waitUpstreams(c.ctx, upIDs); err != nil {
				c.logger.Infof("replay: upstream skip for rule=%q: %v", rnr.rule.Record, err)
				c.setOutcome(rnr.rule.ID, OutcomeFailed)
				return
			}
			rnr.Run(c.ctx)
		}(runner, graph.nodes[id].upstreams)
	}
}
