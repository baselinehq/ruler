package ruler

import (
	"context"
	"sync"
)

// replayCoordinator owns the lifecycle of replay runners and maintains
// per-process state (probed set, outcomes, in-memory progress).
type replayCoordinator struct {
	cfg    ReplayConfig
	qb     QuerierBuilder
	writer SeriesWriter
	logger Logger

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
func newReplayCoordinator(parent context.Context, cfg ReplayConfig, qb QuerierBuilder, writer SeriesWriter, logger Logger) (*replayCoordinator, error) {
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
		cfg:        cfg,
		qb:         qb,
		writer:     writer,
		logger:     ensureLogger(logger),
		httpClient: httpClient,
		probed:     map[uint64]struct{}{},
		outcomes:   map[uint64]ReplayOutcome{},
		doneChans:  map[uint64]chan struct{}{},
		progress:   map[uint64]int64{},
		rulesSem:   make(chan struct{}, cfg.RulesConcurrency),
		ctx:        ctx,
		cancel:     cancel,
		metrics:    newReplayMetrics(cfg.Registerer),
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

// setOutcome stores terminal outcome + closes the rule's done channel (idempotent).
func (c *replayCoordinator) setOutcome(ruleID uint64, outcome ReplayOutcome) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.outcomes[ruleID] = outcome
	if ch, ok := c.doneChans[ruleID]; ok {
		select {
		case <-ch:
		default:
			close(ch)
		}
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

// Temporary stub. Replaced in Phase G with full implementation.
type replayMetrics struct{}

func newReplayMetrics(_ any) *replayMetrics { return nil }
