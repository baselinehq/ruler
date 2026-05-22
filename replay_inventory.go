package ruler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

type metricInventory struct {
	knownNames map[string]struct{}
	fetchedAt  time.Time
}

func (m *metricInventory) Has(name string) bool {
	if m == nil {
		return false
	}
	_, ok := m.knownNames[name]
	return ok
}

// fetchInventory queries /api/v1/label/__name__/values to enumerate every
// metric name currently known to the upstream TSDB. Compatible with
// Prometheus, VictoriaMetrics, Mimir, and Cortex.
func fetchInventory(ctx context.Context, c *HTTPClient) (*metricInventory, error) {
	u := *c.baseURL
	u.Path = strings.TrimSuffix(u.Path, "/")
	u.Path = path.Join(u.Path, "/api/v1/label/__name__/values")
	u.RawQuery = url.Values{}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	for k, v := range c.defaultHeaders {
		req.Header.Set(k, v)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("inventory fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return nil, fmt.Errorf("inventory status=%s body=%q", resp.Status, body)
	}
	var parsed struct {
		Status string   `json:"status"`
		Error  string   `json:"error"`
		Data   []string `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("inventory decode: %w", err)
	}
	if parsed.Status != "success" {
		return nil, fmt.Errorf("inventory response status=%q error=%q", parsed.Status, parsed.Error)
	}
	known := make(map[string]struct{}, len(parsed.Data))
	for _, n := range parsed.Data {
		known[n] = struct{}{}
	}
	return &metricInventory{knownNames: known, fetchedAt: time.Now()}, nil
}

// probeSourceRetention performs a halving binary search to find the rough
// retention boundary for sourceMetric. It queries instant `count(metric @ ts)`
// at increasingly-deep timestamps until it stops returning data.
// Returns the duration of available retention (now - earliestDataTime).
// Cheap by design (~log2(span/hour) queries); precision is roughly 1 day.
func probeSourceRetention(ctx context.Context, q Querier, sourceMetric string, maxProbe time.Duration, now time.Time) (time.Duration, error) {
	expr := fmt.Sprintf("count(%s)", sourceMetric)
	probeAt := func(d time.Duration) (bool, error) {
		res, err := q.Query(ctx, expr, now.Add(-d))
		if err != nil {
			return false, err
		}
		if len(res.Data) == 0 {
			return false, nil
		}
		for _, m := range res.Data {
			for _, v := range m.Values {
				if v > 0 {
					return true, nil
				}
			}
		}
		return false, nil
	}

	// Establish an upper bound: any data within last 5m? If not, no retention.
	if ok, err := probeAt(5 * time.Minute); err != nil || !ok {
		return 0, err
	}

	// Halving search between [0, maxProbe].
	low := time.Duration(0)
	high := maxProbe
	for high-low > 24*time.Hour {
		mid := low + (high-low)/2
		ok, err := probeAt(mid)
		if err != nil {
			return 0, err
		}
		if ok {
			low = mid
		} else {
			high = mid
		}
	}
	return low, nil
}
