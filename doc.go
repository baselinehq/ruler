// Package ruler provides a recording-rules runner for PromQL.
//
// It parses Prometheus rule YAML, schedules rule group evaluation with
// vmalert style options, queries a Prometheus-compatible HTTP API via a Querier,
// and writes the resulting time series through a SeriesWriter (typically
// remote_write).
//
// Only recording rules are supported. Rule updates replace rule instances and
// reset per-rule state, including stale-marker tracking. Stale markers are
// emitted for labelsets missing in the next evaluation using the evaluation
// timestamp.
//
// Implementations of Querier and SeriesWriter must be safe for concurrent use.
package ruler
