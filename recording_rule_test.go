package ruler

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
)

// TestToTimeSeries_AppliesNameLabel verifies that the __name__ label
// is correctly applied to output time series.
func TestToTimeSeries_AppliesNameLabel(t *testing.T) {
	tests := []struct {
		name        string
		inputLabels []prompb.Label
		recordName  string
		wantNameVal string
	}{
		{
			name:        "no __name__ in input",
			inputLabels: []prompb.Label{{Name: "job", Value: "test"}},
			recordName:  "my_metric",
			wantNameVal: "my_metric",
		},
		{
			name: "___name__ in input gets overwritten",
			inputLabels: []prompb.Label{
				{Name: "__name__", Value: "old_metric"},
				{Name: "job", Value: "test"},
			},
			recordName:  "new_metric",
			wantNameVal: "new_metric",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := &RecordingRule{
				name: tt.recordName,
			}

			metric := Metric{
				Labels:     tt.inputLabels,
				Timestamps: []int64{1000},
				Values:     []float64{42.0},
			}

			ts := rule.toTimeSeries(metric)

			// Find __name__ label
			var nameVal string
			for _, lbl := range ts.Labels {
				if lbl.Name == "__name__" {
					nameVal = lbl.Value
					break
				}
			}

			if nameVal != tt.wantNameVal {
				t.Errorf("__name__ = %q, want %q", nameVal, tt.wantNameVal)
			}
		})
	}
}

// TestApplyRuleLabels_ExportedConflict verifies the exported_ label conflict behavior.
func TestApplyRuleLabels_ExportedConflict(t *testing.T) {
	tests := []struct {
		name        string
		inputLabels []prompb.Label
		ruleLabels  map[string]string
		wantLabels  map[string]string // name -> value
	}{
		{
			name: "rule label overrides input, creates exported_",
			inputLabels: []prompb.Label{
				{Name: "job", Value: "a"},
				{Name: "instance", Value: "localhost"},
			},
			ruleLabels: map[string]string{
				"job": "b",
			},
			wantLabels: map[string]string{
				"job":          "b",
				"exported_job": "a",
				"instance":     "localhost",
			},
		},
		{
			name: "exported_ already exists - deterministic collision handling",
			inputLabels: []prompb.Label{
				{Name: "job", Value: "a"},
				{Name: "exported_job", Value: "old"},
			},
			ruleLabels: map[string]string{
				"job": "b",
			},
			wantLabels: map[string]string{
				"job":                   "b",
				"exported_job":          "old",
				"exported_exported_job": "a",
			},
		},
		{
			name: "no conflict when values match",
			inputLabels: []prompb.Label{
				{Name: "job", Value: "a"},
			},
			ruleLabels: map[string]string{
				"job": "a",
			},
			wantLabels: map[string]string{
				"job": "a",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := append([]prompb.Label(nil), tt.inputLabels...)
			sortLabels(input)
			ruleLabels := prepareRuleLabels(tt.ruleLabels)
			result := applyRuleLabels(input, ruleLabels)

			// Convert to map for easier comparison
			got := make(map[string]string)
			for _, lbl := range result {
				got[lbl.Name] = lbl.Value
			}

			// Check all expected labels exist with correct values
			for name, wantVal := range tt.wantLabels {
				gotVal, exists := got[name]
				if !exists {
					t.Errorf("label %q missing in output", name)
					continue
				}
				if gotVal != wantVal {
					t.Errorf("label %q = %q, want %q", name, gotVal, wantVal)
				}
			}

			// Check no extra labels
			if len(got) != len(tt.wantLabels) {
				t.Errorf("got %d labels, want %d labels\nGot: %v\nWant: %v",
					len(got), len(tt.wantLabels), got, tt.wantLabels)
			}
		})
	}
}

func TestLabelsHash_LabelSeparator(t *testing.T) {
	a := []prompb.Label{
		{Name: "a", Value: "b"},
		{Name: "cd", Value: ""},
	}
	b := []prompb.Label{
		{Name: "a", Value: "bc"},
		{Name: "d", Value: ""},
	}
	sortLabels(a)
	sortLabels(b)

	if labelsHash(a) == labelsHash(b) {
		t.Fatal("labelsHash collision due to missing label separator")
	}
}


// TestExec_DuplicateLabelsetAfterLabels tests duplicate detection after label application.
func TestExec_DuplicateLabelsetAfterLabels(t *testing.T) {
	// Create a querier that returns duplicate labelsets
	querier := &testQuerier{
		result: Result{
			Data: []Metric{
				{
					Labels: []prompb.Label{
						{Name: "instance", Value: "same"},
					},
					Timestamps: []int64{1000},
					Values:     []float64{1.0},
				},
				{
					Labels: []prompb.Label{
						{Name: "instance", Value: "same"},
					},
					Timestamps: []int64{1000},
					Values:     []float64{2.0},
				},
			},
		},
	}

	rule := &RecordingRule{
		name:  "test_metric",
		q:     querier,
		state: newRuleState(1),
	}

	_, err := rule.exec(context.Background(), time.Now(), 0)
	if err == nil {
		t.Fatal("expected errDuplicateLabelset, got nil")
	}

	// Check that error contains the duplicate labelset error
	if err.Error() == "" || !strings.Contains(err.Error(), "duplicate labelset") {
		t.Errorf("error should mention duplicate labelset, got: %v", err)
	}
}

// TestExec_EmitsStaleForMissingSeries tests stale marker emission.
func TestExec_EmitsStaleForMissingSeries(t *testing.T) {
	querier := &testQuerier{}

	rule := &RecordingRule{
		name:  "test_metric",
		q:     querier,
		state: newRuleState(1),
	}

	evalTS1 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	evalTS2 := time.Date(2024, 1, 1, 12, 1, 0, 0, time.UTC)

	// First eval: returns series A and B
	querier.result = Result{
		Data: []Metric{
			{
				Labels:     []prompb.Label{{Name: "series", Value: "A"}},
				Timestamps: []int64{evalTS1.UnixMilli()},
				Values:     []float64{1.0},
			},
			{
				Labels:     []prompb.Label{{Name: "series", Value: "B"}},
				Timestamps: []int64{evalTS1.UnixMilli()},
				Values:     []float64{2.0},
			},
		},
	}

	tss1, err := rule.exec(context.Background(), evalTS1, 0)
	if err != nil {
		t.Fatalf("first exec failed: %v", err)
	}
	if len(tss1) != 2 {
		t.Fatalf("first exec: got %d series, want 2", len(tss1))
	}

	// Second eval: returns only series A
	querier.result = Result{
		Data: []Metric{
			{
				Labels:     []prompb.Label{{Name: "series", Value: "A"}},
				Timestamps: []int64{evalTS2.UnixMilli()},
				Values:     []float64{3.0},
			},
		},
	}

	tss2, err := rule.exec(context.Background(), evalTS2, 0)
	if err != nil {
		t.Fatalf("second exec failed: %v", err)
	}

	// Should have 2 series: A (normal) + B (stale)
	if len(tss2) != 2 {
		t.Fatalf("second exec: got %d series, want 2 (A normal + B stale)", len(tss2))
	}

	// Find the stale series (series B)
	var staleSeries *prompb.TimeSeries
	for i := range tss2 {
		for _, lbl := range tss2[i].Labels {
			if lbl.Name == "series" && lbl.Value == "B" {
				staleSeries = &tss2[i]
				break
			}
		}
	}

	if staleSeries == nil {
		t.Fatal("stale series for B not found in output")
	}

	// Verify stale marker
	if len(staleSeries.Samples) != 1 {
		t.Fatalf("stale series has %d samples, want 1", len(staleSeries.Samples))
	}

	sample := staleSeries.Samples[0]

	// Check timestamp is evalTS2
	if sample.Timestamp != evalTS2.UnixMilli() {
		t.Errorf("stale sample timestamp = %d, want %d (evalTS2)",
			sample.Timestamp, evalTS2.UnixMilli())
	}

	// Check value is StaleNaN
	if !value.IsStaleNaN(sample.Value) {
		t.Errorf("stale sample value is not StaleNaN, got float64 bits: %x",
			math.Float64bits(sample.Value))
	}
}

// TestExec_LastEvaluationNotCorruptedOnError is a regression test for the bug where
// exec() mutated lastEvaluation in-place, causing state corruption when errors occurred.
//
// Scenario:
// 1. First eval returns A+B → lastEvaluation = {A, B}
// 2. Second eval returns duplicate labelset → error, but lastEvaluation should still be {A, B}
// 3. Third eval returns only A → should emit stale marker for B
//
// Before the fix, step 2 would corrupt lastEvaluation by deleting A,
// causing step 3 to not emit a stale marker for B.
func TestExec_LastEvaluationNotCorruptedOnError(t *testing.T) {
	querier := &testQuerier{}

	rule := &RecordingRule{
		name:  "test_metric",
		q:     querier,
		state: newRuleState(1),
	}

	evalTS1 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	evalTS2 := time.Date(2024, 1, 1, 12, 1, 0, 0, time.UTC)
	evalTS3 := time.Date(2024, 1, 1, 12, 2, 0, 0, time.UTC)

	// First eval: returns series A and B successfully
	querier.result = Result{
		Data: []Metric{
			{
				Labels:     []prompb.Label{{Name: "series", Value: "A"}},
				Timestamps: []int64{evalTS1.UnixMilli()},
				Values:     []float64{1.0},
			},
			{
				Labels:     []prompb.Label{{Name: "series", Value: "B"}},
				Timestamps: []int64{evalTS1.UnixMilli()},
				Values:     []float64{2.0},
			},
		},
	}

	tss1, err := rule.exec(context.Background(), evalTS1, 0)
	if err != nil {
		t.Fatalf("first exec failed: %v", err)
	}
	if len(tss1) != 2 {
		t.Fatalf("first exec: got %d series, want 2", len(tss1))
	}

	// Second eval: returns series A (seen before) and then C and C (duplicate)
	// This should error BUT NOT corrupt lastEvaluation
	querier.result = Result{
		Data: []Metric{
			{
				Labels:     []prompb.Label{{Name: "series", Value: "A"}},
				Timestamps: []int64{evalTS2.UnixMilli()},
				Values:     []float64{3.0},
			},
			{
				Labels:     []prompb.Label{{Name: "series", Value: "C"}},
				Timestamps: []int64{evalTS2.UnixMilli()},
				Values:     []float64{4.0},
			},
			{
				Labels:     []prompb.Label{{Name: "series", Value: "C"}},
				Timestamps: []int64{evalTS2.UnixMilli()},
				Values:     []float64{5.0},
			},
		},
	}

	_, err = rule.exec(context.Background(), evalTS2, 0)
	if err == nil {
		t.Fatal("second exec should have failed with duplicate labelset")
	}
	if !strings.Contains(err.Error(), "duplicate labelset") {
		t.Errorf("error should mention duplicate labelset, got: %v", err)
	}

	// Third eval: returns only series A
	// This should emit stale marker for B (because B existed in first eval but not in third)
	// REGRESSION: Before the fix, lastEvaluation was corrupted during the failed second eval,
	// so B's stale marker would not be emitted
	querier.result = Result{
		Data: []Metric{
			{
				Labels:     []prompb.Label{{Name: "series", Value: "A"}},
				Timestamps: []int64{evalTS3.UnixMilli()},
				Values:     []float64{6.0},
			},
		},
	}

	tss3, err := rule.exec(context.Background(), evalTS3, 0)
	if err != nil {
		t.Fatalf("third exec failed: %v", err)
	}

	// Should have 2 series: A (normal) + B (stale)
	if len(tss3) != 2 {
		t.Fatalf("third exec: got %d series, want 2 (A normal + B stale)", len(tss3))
	}

	// Find the stale series (series B)
	var staleSeries *prompb.TimeSeries
	var normalSeries *prompb.TimeSeries
	for i := range tss3 {
		for _, lbl := range tss3[i].Labels {
			if lbl.Name == "series" && lbl.Value == "B" {
				staleSeries = &tss3[i]
			}
			if lbl.Name == "series" && lbl.Value == "A" {
				normalSeries = &tss3[i]
			}
		}
	}

	if normalSeries == nil {
		t.Fatal("normal series A not found in third eval")
	}
	if staleSeries == nil {
		t.Fatal("stale series B not found in third eval - lastEvaluation was corrupted by second eval error")
	}

	// Verify stale marker
	if len(staleSeries.Samples) != 1 {
		t.Fatalf("stale series has %d samples, want 1", len(staleSeries.Samples))
	}

	sample := staleSeries.Samples[0]

	// Check timestamp is evalTS3
	if sample.Timestamp != evalTS3.UnixMilli() {
		t.Errorf("stale sample timestamp = %d, want %d (evalTS3)",
			sample.Timestamp, evalTS3.UnixMilli())
	}

	// Check value is StaleNaN
	if !value.IsStaleNaN(sample.Value) {
		t.Errorf("stale sample value is not StaleNaN, got float64 bits: %x",
			math.Float64bits(sample.Value))
	}
}

