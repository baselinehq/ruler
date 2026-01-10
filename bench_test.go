package ruler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

var (
	sinkSeries prompb.TimeSeries
	sinkLabels []prompb.Label
)

func BenchmarkRecordingRule_toTimeSeries(b *testing.B) {
	cases := []struct {
		name    string
		labels  int
		samples int
		// Test different code paths
		hasName        bool // __name__ already in input labels
		forceCollision bool // Force exported_ collision
	}{
		{name: "labels_5_samples_1", labels: 5, samples: 1},
		{name: "labels_20_samples_1", labels: 20, samples: 1},
		{name: "labels_5_samples_10", labels: 5, samples: 10},
		{name: "labels_20_samples_10", labels: 20, samples: 10},
		{name: "name_present_labels_10", labels: 10, samples: 1, hasName: true},
		{name: "collision_labels_10", labels: 10, samples: 1, forceCollision: true},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()

			lbls := makeLabels(c.labels)
			if c.hasName {
				// Add __name__ to test that code path
				lbls = append([]prompb.Label{{Name: "__name__", Value: "existing"}}, lbls...)
			}

			metric := Metric{
				Labels:     lbls,
				Timestamps: makeTimestamps(c.samples),
				Values:     makeValues(c.samples),
			}

			ruleLabels := map[string]string{"env": "bench"}
			if c.forceCollision {
				// Force exported_ collision by having input label that conflicts with rule label
				metric.Labels = append(metric.Labels, prompb.Label{Name: "env", Value: "input"})
			}

			rule := &RecordingRule{
				name:       "bench_metric",
				ruleLabels: prepareRuleLabels(ruleLabels),
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sinkSeries = rule.toTimeSeries(metric)
			}
		})
	}
}

func BenchmarkApplyRuleLabels(b *testing.B) {
	cases := []struct {
		name       string
		numLabels  int
		numRule    int
		collisions int // Number of conflicting labels
	}{
		{name: "no_rules", numLabels: 10, numRule: 0},
		{name: "small_no_collision", numLabels: 5, numRule: 3, collisions: 0},
		{name: "small_with_collision", numLabels: 5, numRule: 3, collisions: 1},
		{name: "medium_no_collision", numLabels: 20, numRule: 10, collisions: 0},
		{name: "medium_with_collisions", numLabels: 20, numRule: 10, collisions: 3},
		{name: "large_no_collision", numLabels: 50, numRule: 20, collisions: 0},
		{name: "large_with_collisions", numLabels: 50, numRule: 20, collisions: 5},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()

			// Build input labels
			labels := makeLabels(c.numLabels)
			sortLabels(labels)

			// Build rule labels
			ruleLabels := make(map[string]string, c.numRule)
			for i := 0; i < c.numRule; i++ {
				key := "r" + strconv.Itoa(i)
				if i < c.collisions {
					// Create collision with existing label
					key = "l" + strconv.Itoa(i)
				}
				ruleLabels[key] = "rv" + strconv.Itoa(i)
			}
			ruleLabelSlice := prepareRuleLabels(ruleLabels)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Copy labels each iteration since applyRuleLabels mutates the slice
				in := append([]prompb.Label(nil), labels...)
				sinkLabels = applyRuleLabels(in, ruleLabelSlice)
			}
		})
	}
}

func BenchmarkLabelsHash(b *testing.B) {
	for _, n := range []int{5, 20, 50} {
		b.Run("labels_"+strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			labels := makeLabels(n)
			sortLabels(labels)
			b.ResetTimer()
			var sinkHash uint64
			for i := 0; i < b.N; i++ {
				sinkHash = labelsHash(labels)
			}
			_ = sinkHash
		})
	}
}

func BenchmarkRecordingRuleExec(b *testing.B) {
	cases := []struct {
		name         string
		series       int
		withLastEval bool // Test stale marker diff cost
	}{
		{name: "series_10_no_stale", series: 10, withLastEval: false},
		{name: "series_10_with_stale", series: 10, withLastEval: true},
		{name: "series_100_no_stale", series: 100, withLastEval: false},
		{name: "series_100_with_stale", series: 100, withLastEval: true},
		{name: "series_1000_no_stale", series: 1000, withLastEval: false},
		{name: "series_1000_with_stale", series: 1000, withLastEval: true},
		{name: "series_10000_no_stale", series: 10000, withLastEval: false},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()

			// Create fake querier with prebuilt result set
			querier := newBenchQuerier(c.series)

			rule := &RecordingRule{
				name:       "bench_metric",
				expr:       "up",
				ruleLabels: prepareRuleLabels(map[string]string{"env": "bench"}),
				q:          querier,
				state:      newRuleState(1),
			}

			// Pre-populate lastEvaluation to measure stale marker diff cost
			if c.withLastEval {
				rule.lastEvaluation = make(map[uint64][][]prompb.Label, c.series/2)
				for i := 0; i < c.series/2; i++ {
					lbls := []prompb.Label{
						{Name: "__name__", Value: "bench_metric"},
						{Name: "series_id", Value: strconv.Itoa(i + 10000)}, // Different IDs to force stale markers
					}
					hash := labelsHash(lbls)
					rule.lastEvaluation[hash] = append(rule.lastEvaluation[hash], lbls)
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := rule.exec(benchCtx, benchTime, 0)
				if err != nil {
					b.Fatalf("exec failed: %v", err)
				}
			}
		})
	}
}

func BenchmarkHTTPClient_parseRangeResponse(b *testing.B) {
	cases := []struct {
		name    string
		series  int
		samples int
	}{
		{name: "small_10x10", series: 10, samples: 10},
		{name: "medium_100x10", series: 100, samples: 10},
		{name: "medium_10x100", series: 10, samples: 100},
		{name: "large_100x100", series: 100, samples: 100},
		{name: "large_1000x10", series: 1000, samples: 10},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()

			// Build realistic JSON payload
			payload := buildRangeResponseJSON(c.series, c.samples)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resp := &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(bytes.NewReader(payload)),
					Header:     http.Header{"Content-Type": []string{"application/json"}},
				}
				_, err := parseRangeResponse(resp)
				if err != nil {
					b.Fatalf("parse failed: %v", err)
				}
			}
		})
	}
}

func BenchmarkExecConcurrently_WithWork(b *testing.B) {
	// Create 100 rules for a more realistic scenario
	const numRules = 100
	rules := make([]*RecordingRule, numRules)
	querier := newBenchQuerier(100)

	for i := range rules {
		rules[i] = &RecordingRule{
			name:       "bench_metric",
			expr:       "up",
			ruleLabels: prepareRuleLabels(map[string]string{"env": "bench"}),
			q:          querier,
			state:      newRuleState(1),
		}
	}

	// exec rule
	workFn := func(rule *RecordingRule) error {
		_, err := rule.exec(benchCtx, benchTime, 0)
		return err
	}

	for _, conc := range []int{1, 10, 50} {
		b.Run("concurrency_"+strconv.Itoa(conc), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				errs := execConcurrently(rules, conc, workFn)
				for err := range errs {
					if err != nil {
						b.Fatalf("exec failed: %v", err)
					}
				}
			}
		})
	}
}

// Benchmark helpers

var (
	benchCtx  = context.Background()
	benchTime = time.Unix(1000, 0)
)

func newBenchQuerier(seriesCount int) *testQuerier {
	metrics := make([]Metric, seriesCount)
	for i := range metrics {
		metrics[i] = Metric{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "bench_metric"},
				{Name: "series_id", Value: strconv.Itoa(i)},
			},
			Timestamps: []int64{1000},
			Values:     []float64{float64(i)},
		}
	}
	return &testQuerier{
		result: Result{Data: metrics},
	}
}

func buildRangeResponseJSON(numSeries, numSamples int) []byte {
	var buf bytes.Buffer
	buf.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)

	for s := 0; s < numSeries; s++ {
		if s > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, `{"metric":{"__name__":"bench_metric","series_id":"%d"},"values":[`, s)

		for i := 0; i < numSamples; i++ {
			if i > 0 {
				buf.WriteByte(',')
			}
			// [timestamp, value]
			fmt.Fprintf(&buf, `[%d,"%d"]`, 1000+i, s*100+i)
		}
		buf.WriteString("]}")
	}

	buf.WriteString(`]}}`)
	return buf.Bytes()
}
