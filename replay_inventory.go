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
