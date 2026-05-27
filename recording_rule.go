package ruler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
)

var errDuplicateLabelset = errors.New("duplicate labelset after applying labels")

// RecordingRule evaluates a single recording rule expression and emits time series.
type RecordingRule struct {
	id         uint64
	name       string
	expr       string
	ruleLabels []prompb.Label
	groupID    uint64
	groupName  string
	file       string
	debug      bool

	q Querier

	state          *ruleState
	lastEvaluation map[uint64][][]prompb.Label
}

func newRecordingRule(qb QuerierBuilder, group *groupRunner, cfg Rule, labels map[string]string, debug bool) *RecordingRule {
	q := qb.Build(QueryParams{
		EvaluationInterval: group.interval,
		QueryParams:        group.params,
		Headers:            group.headers,
		Debug:              debug,
	})

	entrySize := group.updateEntriesLimit
	if cfg.UpdateEntriesLimit != nil && *cfg.UpdateEntriesLimit > 0 {
		entrySize = *cfg.UpdateEntriesLimit
	}
	if entrySize < 1 {
		entrySize = 1
	}

	ruleLabels := prepareRuleLabels(labels)
	return &RecordingRule{
		id:         cfg.ID,
		name:       cfg.Record,
		expr:       cfg.Expr,
		ruleLabels: ruleLabels,
		groupID:    group.id,
		groupName:  group.name,
		file:       group.file,
		debug:      debug,
		q:          q,
		state:      newRuleState(entrySize),
	}
}

func (r *RecordingRule) exec(ctx context.Context, ts time.Time, limit int) ([]prompb.TimeSeries, error) {
	start := time.Now()
	res, err := r.q.Query(ctx, r.expr, ts)

	state := StateEntry{
		Time:     start,
		At:       ts,
		Duration: time.Since(start),
		Samples:  len(res.Data),
	}

	if err != nil {
		state.Err = fmt.Errorf("failed to execute query %q: %w", r.expr, err)
		r.state.add(state)
		return nil, state.Err
	}

	if limit > 0 && len(res.Data) > limit {
		state.Err = fmt.Errorf("exec exceeded limit of %d with %d series", limit, len(res.Data))
		r.state.add(state)
		return nil, state.Err
	}

	tss, curEvaluation, err := r.toTimeSeriesSet(res.Data)
	if err != nil {
		state.Err = err
		r.state.add(state)
		return nil, fmt.Errorf("recording rule %q: %w", r.name, err)
	}

	// Compute stale series by set difference
	last := r.lastEvaluation
	for hash, lblsSet := range last {
		curSet := curEvaluation[hash]
		for _, lbls := range lblsSet {
			if labelsSetContains(curSet, lbls) {
				continue
			}
			// Series from last eval not in current eval → emit stale marker
			tss = append(tss, prompb.TimeSeries{
				Labels: lbls,
				Samples: []prompb.Sample{{
					Value:     math.Float64frombits(value.StaleNaN),
					Timestamp: ts.UnixMilli(),
				}},
			})
		}
	}

	// Only update lastEvaluation after all checks pass
	r.lastEvaluation = curEvaluation

	r.state.add(state)
	return tss, nil
}

func (r *RecordingRule) toTimeSeriesSet(data []Metric) ([]prompb.TimeSeries, map[uint64][][]prompb.Label, error) {
	seen := make(map[uint64][][]prompb.Label, len(data))
	tss := make([]prompb.TimeSeries, 0, len(data))
	for _, m := range data {
		ts := r.toTimeSeries(m)
		hash := labelsHash(ts.Labels)
		if labelsSetContains(seen[hash], ts.Labels) {
			return nil, nil, errDuplicateLabelset
		}
		seen[hash] = append(seen[hash], ts.Labels)
		tss = append(tss, ts)
	}
	return tss, seen, nil
}

// sortLabels sorts labels by name using insertion sort.
// Insertion sort is optimal for small N (typical label counts: 5-20).
// Avoids sort.Slice reflect overhead.
func sortLabels(labels []prompb.Label) {
	for i := 1; i < len(labels); i++ {
		j := i
		for j > 0 && labels[j-1].Name > labels[j].Name {
			labels[j-1], labels[j] = labels[j], labels[j-1]
			j--
		}
	}
}

func (r *RecordingRule) toTimeSeries(m Metric) prompb.TimeSeries {
	conflictCap := len(r.ruleLabels)
	if max := len(m.Labels) + 1; conflictCap > max {
		conflictCap = max
	}
	labels := make([]prompb.Label, 0, len(m.Labels)+1+len(r.ruleLabels)+conflictCap)
	labels = append(labels, m.Labels...)
	labels = applyName(labels, r.name)
	sortLabels(labels)
	labels = applyRuleLabels(labels, r.ruleLabels)

	samples := make([]prompb.Sample, 0, len(m.Timestamps))
	for i := range m.Timestamps {
		samples = append(samples, prompb.Sample{
			Value:     m.Values[i],
			Timestamp: m.Timestamps[i],
		})
	}
	return prompb.TimeSeries{
		Labels:  labels,
		Samples: samples,
	}
}

func applyName(labels []prompb.Label, name string) []prompb.Label {
	out := labels[:0]
	for i := range labels {
		if labels[i].Name == "__name__" {
			continue
		}
		out = append(out, labels[i])
	}
	out = append(out, prompb.Label{Name: "__name__", Value: name})
	return out
}

// applyRuleLabels merges pre-sorted rule labels into a sorted label set.
// labels and ruleLabels must be sorted by Name.
func applyRuleLabels(labels []prompb.Label, ruleLabels []prompb.Label) []prompb.Label {
	if len(ruleLabels) == 0 {
		return labels
	}

	for i := range ruleLabels {
		ruleLabel := ruleLabels[i]
		idx, exists := findLabelIndexSorted(labels, ruleLabel.Name)
		if !exists {
			labels = insertLabelAt(labels, idx, ruleLabel)
			continue
		}

		if labels[idx].Value == ruleLabel.Value {
			continue
		}

		exportedName := uniqueExportedName(labels, ruleLabels, ruleLabel.Name)
		existing := labels[idx]
		labels = removeLabelAt(labels, idx)
		insertIdx, _ := findLabelIndexSorted(labels, exportedName)
		labels = insertLabelAt(labels, insertIdx, prompb.Label{Name: exportedName, Value: existing.Value})
		insertIdx, _ = findLabelIndexSorted(labels, ruleLabel.Name)
		labels = insertLabelAt(labels, insertIdx, ruleLabel)
	}

	return labels
}

func prepareRuleLabels(ruleLabels map[string]string) []prompb.Label {
	if len(ruleLabels) == 0 {
		return nil
	}
	labels := make([]prompb.Label, 0, len(ruleLabels))
	for k, v := range ruleLabels {
		if v == "" {
			continue
		}
		labels = append(labels, prompb.Label{Name: k, Value: v})
	}
	sortLabels(labels)
	return labels
}

func uniqueExportedName(labels []prompb.Label, ruleLabels []prompb.Label, name string) string {
	exportedName := "exported_" + name
	for labelNameExists(labels, exportedName) || labelNameExists(ruleLabels, exportedName) {
		exportedName = "exported_" + exportedName
	}
	return exportedName
}

func labelNameExists(labels []prompb.Label, name string) bool {
	_, ok := findLabelIndexSorted(labels, name)
	return ok
}

func findLabelIndexSorted(labels []prompb.Label, name string) (int, bool) {
	lo := 0
	hi := len(labels)
	for lo < hi {
		mid := (lo + hi) / 2
		switch {
		case labels[mid].Name == name:
			return mid, true
		case labels[mid].Name < name:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return lo, false
}

func insertLabelAt(labels []prompb.Label, idx int, label prompb.Label) []prompb.Label {
	labels = append(labels, prompb.Label{})
	copy(labels[idx+1:], labels[idx:])
	labels[idx] = label
	return labels
}

func removeLabelAt(labels []prompb.Label, idx int) []prompb.Label {
	copy(labels[idx:], labels[idx+1:])
	labels[len(labels)-1] = prompb.Label{}
	return labels[:len(labels)-1]
}

func labelsSetContains(set [][]prompb.Label, labels []prompb.Label) bool {
	for i := range set {
		if labelsEqual(set[i], labels) {
			return true
		}
	}
	return false
}

// labelsHash computes a fast hash over sorted labels.
// Uses FNV-1a for good distribution with minimal allocation.
func labelsHash(labels []prompb.Label) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	const labelSep = 0xff

	hash := uint64(offset64)
	for i := range labels {
		// Hash name
		for j := 0; j < len(labels[i].Name); j++ {
			hash ^= uint64(labels[i].Name[j])
			hash *= prime64
		}
		// Separator
		hash ^= uint64('=')
		hash *= prime64
		// Hash value
		for j := 0; j < len(labels[i].Value); j++ {
			hash ^= uint64(labels[i].Value[j])
			hash *= prime64
		}
		// Separator between labels to avoid ambiguous concatenation.
		hash ^= labelSep
		hash *= prime64
	}
	return hash
}

// labelsEqual checks if two label slices are equal.
// Used for collision verification with labelsHash.
func labelsEqual(a, b []prompb.Label) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Value != b[i].Value {
			return false
		}
	}
	return true
}

// StateEntry captures a single rule evaluation outcome.
type StateEntry struct {
	Time     time.Time
	At       time.Time
	Duration time.Duration
	Samples  int
	Err      error
}

type ruleState struct {
	mu      sync.Mutex
	entries []StateEntry
	next    int
	filled  bool
}

func newRuleState(size int) *ruleState {
	return &ruleState{
		entries: make([]StateEntry, size),
	}
}

func (s *ruleState) add(e StateEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[s.next] = e
	s.next++
	if s.next >= len(s.entries) {
		s.next = 0
		s.filled = true
	}
}
