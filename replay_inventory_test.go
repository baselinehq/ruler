package ruler

import (
	"encoding/json"
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
