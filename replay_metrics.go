package ruler

import (
	"github.com/prometheus/client_golang/prometheus"
)

type replayMetrics struct {
	RulesTotal       *prometheus.CounterVec
	ChunksTotal      *prometheus.CounterVec
	ChunkDuration    *prometheus.HistogramVec
	SamplesWritten   *prometheus.CounterVec
	GapsDetected     *prometheus.HistogramVec
	SpanResolved     *prometheus.GaugeVec
	UpstreamWaitSecs *prometheus.HistogramVec
	Active           prometheus.Gauge
	InventoryAge     prometheus.Gauge
}

func newReplayMetrics(reg prometheus.Registerer) *replayMetrics {
	m := &replayMetrics{
		RulesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ruler_replay_rules_total",
			Help: "Count of rule replay attempts by terminal outcome.",
		}, []string{"outcome"}),
		ChunksTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ruler_replay_chunks_total",
			Help: "Count of chunks executed by outcome.",
		}, []string{"rule", "outcome"}),
		ChunkDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ruler_replay_chunk_duration_seconds",
			Help:    "Chunk execution time.",
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 12),
		}, []string{"rule"}),
		SamplesWritten: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ruler_replay_samples_written_total",
			Help: "Samples remote-written by replay.",
		}, []string{"rule"}),
		GapsDetected: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ruler_replay_gap_seconds",
			Help:    "Gap durations detected in probe output.",
			Buckets: prometheus.ExponentialBuckets(60, 4, 10),
		}, []string{"rule"}),
		SpanResolved: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ruler_replay_span_seconds",
			Help: "Resolved replay span per rule.",
		}, []string{"rule"}),
		UpstreamWaitSecs: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ruler_replay_upstream_wait_seconds",
			Help:    "Time spent waiting on upstream rule completion.",
			Buckets: prometheus.ExponentialBuckets(0.01, 4, 10),
		}, []string{"rule"}),
		Active: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ruler_replay_active_runners",
			Help: "Number of replay runners currently active.",
		}),
		InventoryAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ruler_replay_inventory_age_seconds",
			Help: "Seconds since last metric inventory fetch.",
		}),
	}
	if reg != nil {
		reg.MustRegister(m.RulesTotal, m.ChunksTotal, m.ChunkDuration, m.SamplesWritten, m.GapsDetected, m.SpanResolved, m.UpstreamWaitSecs, m.Active, m.InventoryAge)
	}
	return m
}
