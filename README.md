# ruler

A Go library to run PromQL recording rules. It parses Prometheus rule YAML, schedules evaluation with vmalert-style group options, queries a Prometheus-compatible HTTP API, and writes resulting time series via a pluggable `SeriesWriter`.

## Install

```bash
go get github.com/baselinehq/ruler
```

## Usage

### Example rule file

```yaml
groups:
- name: node_derived_5m
  type: prometheus
  interval: 1m
  eval_delay: 30s
  eval_alignment: true
  concurrency: 4
  limit: 2000
  labels:
    cluster: prod
  params:
    timeout: "30s"
  headers:
    - "X-Scope-OrgID: tenant-a"
  rules:
    - record: tenant_instance:node_cpu_cores
      expr: count by (instance) (node_cpu_seconds_total{mode="idle"})
      labels:
        metric_type: cpu
```

### Example (Manager + Apply + Stop)

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"github.com/baselinehq/ruler"
)

type logWriter struct{}

func (w *logWriter) Write(ctx context.Context, series []prompb.TimeSeries) error {
	log.Printf("writing %d series", len(series))
	return nil
}

type Logger struct{}

func (Logger) Infof(format string, args ...any)  { log.Printf("[INFO] "+format, args...) }

func main() {
	cfg, err := ruler.ParseConfigFile("rules.yaml")
	if err != nil {
		log.Fatal(err)
	}

	httpClient, err := ruler.NewHTTPClient(ruler.HTTPConfig{
		URL:     "http://prometheus:9090",
		Timeout: 30 * time.Second,
		Logger:  Logger{},
	})
	if err != nil {
		log.Fatal(err)
	}

	mgr, err := ruler.NewManager(ruler.ManagerConfig{
		QuerierBuilder:     httpClient,
		Writer:             &logWriter{},
		Context:            context.Background(),
		EvaluationInterval: time.Minute,
		Logger:             Logger{},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer mgr.Stop()

	if err := mgr.Apply(*cfg); err != nil {
		log.Fatal(err)
	}

	// Block until shutdown.
	select {}
}
```

## Configuration

- **Group defaults**
  - `type`: defaults to `prometheus` (only supported type).
  - `interval`: uses `ManagerConfig.EvaluationInterval` when unset or 0.
  - `eval_delay`: uses `ManagerConfig.EvalDelay` when unset.
  - `concurrency`: values <1 are treated as 1.
  - `limit`: 0 means no limit.
  - `eval_alignment`: nil behaves as `true`.
- **Rule defaults**
  - `record` + `expr` are required. `alert` is rejected.
  - `labels`: optional.
  - `update_entries_limit`: overrides the manager default for internal rule-state history.
- **Headers / Params**
  - `headers`: list of `"Key: Value"` strings.
  - `params`: map of query parameters. Values may be scalars or lists.

### Scheduling

- **interval**: group evaluation period. Must be >0 after defaults are applied.
- **eval_delay**: if `eval_offset` is not set, the query timestamp is `evalTS - eval_delay`.
- **eval_alignment**: if nil or true, the query timestamp is truncated to the interval after applying `eval_delay`. If false, no truncation.
- **eval_offset**: aligns group start to `truncate(interval) + eval_offset`. When set, `eval_delay` is ignored and query timestamps use the scheduled eval time directly.
- **max_start_delay** (manager): if `eval_offset` is not set, group start is delayed by a deterministic jitter within `min(interval, max_start_delay)`.

### Missed ticks

The scheduler advances to the next interval boundary and skips missed intervals. It does not backfill, each tick results in at most one evaluation.

### Updates

- `Manager.Apply` creates, updates, and stops groups.
- Updates are sent via a buffered channel with “latest wins” semantics: if a new update arrives while a previous one is pending, the old one is dropped.
- Updates cancel in-flight evaluations and swap in new rule objects.
- State reset on update: new rule instances reset `lastEvaluation` and rule history, stale markers are not emitted across updates.

### State

- Each evaluation records labelsets seen in that run.
- If a labelset existed in the previous evaluation but not the current one, the runner emits a single sample with `StaleNaN` and the evaluation timestamp in milliseconds.
- Labelset identity uses a fast hash with collision-safe chaining, equality is verified before treating series as duplicates.

### Concurrency

`concurrency` is the maximum number of rules evaluated concurrently per group. The `Querier` and `SeriesWriter` implementations must be safe for concurrent use.

## Label Semantics

- **`__name__`**: any existing `__name__` label is removed, `__name__` is then set to the rule’s `record` name.
- **Rule label merge**:
  - Rule labels are merged into the series label set.
  - If a rule label conflicts with an existing label of a different value, the existing label is renamed to `exported_<name>`, and the rule label is inserted.
  - If `exported_<name>` already exists, the prefix is repeated (`exported_exported_<name>`, etc.).
  - Empty rule-label values are skipped.

## Datasource (Prometheus HTTP API)

The built-in `HTTPClient` implements `QuerierBuilder` and `Querier`:

- **Endpoints**:
  - `/api/v1/query` for instant queries.
  - `/api/v1/query_range` for range queries.
  - Path suffixes are appended unless `DisablePathAppend` is set or the URL already ends with the suffix.
- **Result types**:
  - Instant: `vector`, `scalar`.
  - Range: `matrix`.
- **Errors**:
  - Non-2xx HTTP responses return an error including status and body (up to 4KiB).
  - `{"status":"error"}` responses return an error with `errorType` and `error`.
- **Stats**:
  - `Result.SeriesFetched` and `Result.IsPartial` are parsed (if present) and returned to callers, the runner does not use them.

## Performance & Benchmarks

Run benchmarks for the package:

```bash
go test -run=^$ -bench=. -benchmem ./ruler
```

To capture CPU/memory profiles:

```bash
GOMAXPROCS=1 go test -run=^$ -bench=. -benchmem -cpuprofile=cpu.prof -memprofile=mem.prof ./ruler
```

Interpretation:
- `ns/op` is time per operation.
- `B/op` and `allocs/op` show memory pressure, lower is better.

## Misc
- Recording rules only, alerting rules are rejected.
- Updates reset per-rule state, stale markers are not emitted across updates.
- Only Prometheus-compatible HTTP API is provided in this package, other datasources require a custom `QuerierBuilder`.
- Per-rule evaluation history is internal, there is no public API to read it.
