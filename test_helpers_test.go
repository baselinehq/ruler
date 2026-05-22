package ruler

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

type testQuerier struct {
	result Result
	err    error
}

func (q *testQuerier) Build(params QueryParams) Querier {
	return q
}

func (q *testQuerier) Query(ctx context.Context, query string, ts time.Time) (Result, error) {
	return q.result, q.err
}

func (q *testQuerier) QueryRange(ctx context.Context, query string, start, end time.Time) (Result, error) {
	return q.result, q.err
}

type testCapturingWriter struct {
	writes [][]prompb.TimeSeries
}

func (w *testCapturingWriter) Write(ctx context.Context, tss []prompb.TimeSeries) error {
	w.writes = append(w.writes, tss)
	return nil
}

type testCountingWriter struct {
	count  int32
	notify chan struct{}
}

func (w *testCountingWriter) Write(ctx context.Context, tss []prompb.TimeSeries) error {
	atomic.AddInt32(&w.count, 1)
	if w.notify != nil {
		select {
		case w.notify <- struct{}{}:
		default:
		}
	}
	return nil
}

type testNoopWriter struct{}

func (w *testNoopWriter) Write(ctx context.Context, tss []prompb.TimeSeries) error {
	return nil
}

type testLogger struct {
	t *testing.T
}

func (l *testLogger) Debugf(format string, args ...any) {
	l.t.Helper()
	l.t.Logf("[DEBUG] "+format, args...)
}

func (l *testLogger) Infof(format string, args ...any) {
	l.t.Helper()
	l.t.Logf("[INFO] "+format, args...)
}

func (l *testLogger) Warnf(format string, args ...any) {
	l.t.Helper()
	l.t.Logf("[WARN] "+format, args...)
}

func (l *testLogger) Errorf(format string, args ...any) {
	l.t.Helper()
	l.t.Logf("[ERROR] "+format, args...)
}

func makeLabels(n int) []prompb.Label {
	lbls := make([]prompb.Label, 0, n)
	for i := 0; i < n; i++ {
		lbls = append(lbls, prompb.Label{
			Name:  "l" + strconv.Itoa(i),
			Value: "v" + strconv.Itoa(i),
		})
	}
	return lbls
}

func makeTimestamps(n int) []int64 {
	ts := make([]int64, n)
	for i := range ts {
		ts[i] = int64(i + 1)
	}
	return ts
}

func makeValues(n int) []float64 {
	vals := make([]float64, n)
	for i := range vals {
		vals[i] = float64(i + 1)
	}
	return vals
}

// testRangeQuerier serves canned matrix responses keyed by query string,
// with optional per-query error injection. QueryRange honors the [start,end]
// window by filtering timestamps in range.
type testRangeQuerier struct {
	mu          sync.Mutex
	responses   map[string][]Metric
	rangeErr    error
	probeFunc   func(query string, start, end time.Time) (Result, error)
	instantFunc func(query string, ts time.Time) (Result, error)
}

func (q *testRangeQuerier) Build(params QueryParams) Querier { return q }

func (q *testRangeQuerier) Query(ctx context.Context, query string, ts time.Time) (Result, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.instantFunc != nil {
		return q.instantFunc(query, ts)
	}
	if data, ok := q.responses[query]; ok {
		return Result{Data: data}, nil
	}
	return Result{}, nil
}

func (q *testRangeQuerier) QueryRange(ctx context.Context, query string, start, end time.Time) (Result, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.rangeErr != nil {
		return Result{}, q.rangeErr
	}
	if q.probeFunc != nil {
		return q.probeFunc(query, start, end)
	}
	if data, ok := q.responses[query]; ok {
		out := make([]Metric, 0, len(data))
		for _, m := range data {
			var ts []int64
			var vs []float64
			for i, t := range m.Timestamps {
				if t >= start.UnixMilli() && t <= end.UnixMilli() {
					ts = append(ts, t)
					vs = append(vs, m.Values[i])
				}
			}
			if len(ts) > 0 {
				out = append(out, Metric{Labels: m.Labels, Timestamps: ts, Values: vs})
			}
		}
		return Result{Data: out}, nil
	}
	return Result{}, nil
}

func mustParseConfigBytes(t *testing.T, b []byte) Config {
	cfg, err := ParseConfig(b)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return *cfg
}
