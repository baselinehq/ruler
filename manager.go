package ruler

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ManagerConfig configures the rule manager.
type ManagerConfig struct {
	QuerierBuilder     QuerierBuilder
	Writer             SeriesWriter
	Context            context.Context
	EvaluationInterval time.Duration
	EvalDelay          time.Duration
	MaxStartDelay      time.Duration
	UpdateEntriesLimit int
	ExternalLabels     map[string]string
	Logger             Logger
	Replay             *ReplayConfig
}

// Manager manages recording rule groups.
type Manager struct {
	mu sync.RWMutex

	qb                 QuerierBuilder
	writer             SeriesWriter
	ctx                context.Context
	evaluationInterval time.Duration
	evalDelay          time.Duration
	maxStartDelay      time.Duration
	updateEntriesLimit int
	externalLabels     map[string]string
	logger             Logger

	groups map[uint64]*groupRunner
	replay *replayCoordinator
}

// NewManager creates a new rule manager.
func NewManager(cfg ManagerConfig) (*Manager, error) {
	if cfg.QuerierBuilder == nil {
		return nil, fmt.Errorf("querier builder is required")
	}

	if cfg.Writer == nil {
		return nil, ErrNoWriter
	}

	ctx := cfg.Context
	if ctx == nil {
		ctx = context.Background()
	}

	evalInterval := cfg.EvaluationInterval
	if evalInterval <= 0 {
		evalInterval = time.Minute
	}

	evalDelay := cfg.EvalDelay
	if evalDelay <= 0 {
		evalDelay = 30 * time.Second
	}

	maxStartDelay := cfg.MaxStartDelay
	if maxStartDelay <= 0 {
		maxStartDelay = 5 * time.Minute
	}

	updateEntriesLimit := cfg.UpdateEntriesLimit
	if updateEntriesLimit == 0 {
		updateEntriesLimit = 20
	}

	mgr := &Manager{
		qb:                 cfg.QuerierBuilder,
		writer:             cfg.Writer,
		ctx:                ctx,
		evaluationInterval: evalInterval,
		evalDelay:          evalDelay,
		maxStartDelay:      maxStartDelay,
		updateEntriesLimit: updateEntriesLimit,
		externalLabels:     copyStringMap(cfg.ExternalLabels),
		logger:             ensureLogger(cfg.Logger),
		groups:             make(map[uint64]*groupRunner),
	}

	if cfg.Replay != nil {
		coord, err := newReplayCoordinator(ctx, *cfg.Replay, cfg.QuerierBuilder, cfg.Writer, mgr.logger, mgr.externalLabels)
		if err != nil {
			return nil, fmt.Errorf("replay coordinator: %w", err)
		}
		if coord != nil {
			if client, ok := cfg.QuerierBuilder.(*HTTPClient); ok {
				coord.httpClient = client
			}
			mgr.replay = coord
		}
	}

	return mgr, nil
}

// Apply updates groups with the provided configuration.
func (m *Manager) Apply(cfg Config) error {
	groupsRegistry := make(map[uint64]*groupRunner)
	for i := range cfg.Groups {
		if err := cfg.Groups[i].Validate(); err != nil {
			return fmt.Errorf("group %q: %w", cfg.Groups[i].Name, err)
		}
		ng, err := newGroupRunner(cfg.Groups[i], m)
		if err != nil {
			return err
		}
		// Check for duplicate group identity
		if existing, exists := groupsRegistry[ng.id]; exists {
			return fmt.Errorf("duplicate group identity: %q and %q hash to same id", existing.name, ng.name)
		}
		groupsRegistry[ng.id] = ng
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for id, og := range m.groups {
		ng, ok := groupsRegistry[id]
		if !ok {
			og.Stop()
			delete(m.groups, id)
			continue
		}
		delete(groupsRegistry, id)
		if og.checksum != ng.checksum {
			og.InterruptEval()
			if err := og.UpdateWith(ng); err != nil {
				m.logger.Errorf("group %q update failed: %s", og.name, err)
			}
		}
	}

	for id, ng := range groupsRegistry {
		m.groups[id] = ng
		ng.Start(m.ctx, m.maxStartDelay)
	}

	if m.replay != nil {
		m.replay.applyAsync(m.ctx, cfg)
	}

	return nil
}

// Stop stops all groups managed by the manager.
func (m *Manager) Stop() {
	if m.replay != nil {
		m.replay.Stop()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, g := range m.groups {
		g.Stop()
		delete(m.groups, id)
	}
}
