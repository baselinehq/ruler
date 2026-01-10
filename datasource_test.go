package ruler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPClient_Query_Vector(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" {
			http.NotFound(w, r)
			return
		}
		switch r.URL.Query().Get("query") {
		case "vector":
			w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"b":"2","a":"1"},"value":[1700000000.5,"3.14"]}]}}`))
		default:
			http.Error(w, "unexpected query", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	client, err := NewHTTPClient(HTTPConfig{URL: server.URL})
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	querier := client.Build(QueryParams{})

	res, err := querier.Query(context.Background(), "vector", time.Unix(1700000000, 0))
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Data) != 1 {
		t.Fatalf("got %d metrics, want 1", len(res.Data))
	}

	labels := res.Data[0].Labels
	if len(labels) != 2 {
		t.Fatalf("got %d labels, want 2", len(labels))
	}
	if labels[0].Name != "a" || labels[1].Name != "b" {
		t.Fatalf("labels not sorted: got %v", labels)
	}

	if got := res.Data[0].Timestamps[0]; got != 1700000000500 {
		t.Fatalf("timestamp=%d, want 1700000000500", got)
	}
	if got := res.Data[0].Values[0]; got != 3.14 {
		t.Fatalf("value=%v, want 3.14", got)
	}
}

func TestHTTPClient_Query_Scalar(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" {
			http.NotFound(w, r)
			return
		}
		switch r.URL.Query().Get("query") {
		case "scalar":
			w.Write([]byte(`{"status":"success","data":{"resultType":"scalar","result":[1700000001.25,"2"]}}`))
		default:
			http.Error(w, "unexpected query", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	client, err := NewHTTPClient(HTTPConfig{URL: server.URL})
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	querier := client.Build(QueryParams{})

	res, err := querier.Query(context.Background(), "scalar", time.Unix(1700000001, 0))
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Data) != 1 {
		t.Fatalf("got %d metrics, want 1", len(res.Data))
	}
	if len(res.Data[0].Labels) != 0 {
		t.Fatalf("got %d labels, want 0", len(res.Data[0].Labels))
	}
	if got := res.Data[0].Timestamps[0]; got != 1700000001250 {
		t.Fatalf("timestamp=%d, want 1700000001250", got)
	}
	if got := res.Data[0].Values[0]; got != 2.0 {
		t.Fatalf("value=%v, want 2.0", got)
	}
}

func TestHTTPClient_QueryRange_Matrix(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query_range" {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1700000000,"1"],[1700000060,"2"]]}]}}`))
	}))
	defer server.Close()

	client, err := NewHTTPClient(HTTPConfig{URL: server.URL})
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	querier := client.Build(QueryParams{})

	start := time.Unix(1700000000, 0)
	end := time.Unix(1700000060, 0)
	res, err := querier.QueryRange(context.Background(), "range", start, end)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if len(res.Data) != 1 {
		t.Fatalf("got %d metrics, want 1", len(res.Data))
	}
	if got := res.Data[0].Labels[0].Name; got != "foo" {
		t.Fatalf("label name=%q, want foo", got)
	}
	if got := res.Data[0].Timestamps[0]; got != 1700000000000 {
		t.Fatalf("timestamp[0]=%d, want 1700000000000", got)
	}
	if got := res.Data[0].Timestamps[1]; got != 1700000060000 {
		t.Fatalf("timestamp[1]=%d, want 1700000060000", got)
	}
	if got := res.Data[0].Values[0]; got != 1.0 {
		t.Fatalf("value[0]=%v, want 1.0", got)
	}
	if got := res.Data[0].Values[1]; got != 2.0 {
		t.Fatalf("value[1]=%v, want 2.0", got)
	}
}
