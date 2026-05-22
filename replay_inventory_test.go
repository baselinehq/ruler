package ruler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFetchInventory_ParsesLabelValues(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/label/__name__/values" {
			t.Errorf("path = %q", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data":   []string{"up", "foo", "bar"},
		})
	}))
	defer srv.Close()
	client, err := NewHTTPClient(HTTPConfig{URL: srv.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	inv, err := fetchInventory(t.Context(), client)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	for _, want := range []string{"up", "foo", "bar"} {
		if _, ok := inv.knownNames[want]; !ok {
			t.Errorf("missing %q in inventory", want)
		}
	}
}

func TestFetchInventory_ErrorOn5xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
	}))
	defer srv.Close()
	client, _ := NewHTTPClient(HTTPConfig{URL: srv.URL, Timeout: time.Second})
	if _, err := fetchInventory(t.Context(), client); err == nil {
		t.Fatal("want error on 503")
	}
}

func TestMetricInventory_Has(t *testing.T) {
	inv := &metricInventory{knownNames: map[string]struct{}{"foo": {}, "bar": {}}}
	if !inv.Has("foo") || inv.Has("baz") {
		t.Errorf("Has wrong: %v %v", inv.Has("foo"), inv.Has("baz"))
	}
}

func TestProbeSourceRetention_FindsBoundary(t *testing.T) {
	// Source has data back to ~14d; queries deeper return empty.
	cutoff := time.Now().Add(-14 * 24 * time.Hour)
	now := time.Now()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		timeStr := r.URL.Query().Get("time")
		var queryAt time.Time
		if timeStr != "" {
			sec, _ := parseFloatSeconds(timeStr)
			queryAt = time.Unix(int64(sec), 0)
		}
		empty := queryAt.Before(cutoff)
		var body string
		if empty {
			body = `{"status":"success","data":{"resultType":"scalar","result":[0,"0"]}}`
		} else {
			body = `{"status":"success","data":{"resultType":"scalar","result":[0,"42"]}}`
		}
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()
	client, _ := NewHTTPClient(HTTPConfig{URL: srv.URL, Timeout: time.Second})
	q := client.Build(QueryParams{})
	got, err := probeSourceRetention(t.Context(), q, "foo", 30*24*time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	wantMin := 12 * 24 * time.Hour
	wantMax := 16 * 24 * time.Hour
	if got < wantMin || got > wantMax {
		t.Errorf("retention = %v, want roughly 14d (between %v and %v)", got, wantMin, wantMax)
	}
}

func parseFloatSeconds(s string) (float64, error) {
	var f float64
	_, err := fmt.Sscanf(s, "%f", &f)
	return f, err
}
