package ruler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

// Querier executes PromQL queries.
type Querier interface {
	Query(ctx context.Context, query string, ts time.Time) (Result, error)
	QueryRange(ctx context.Context, query string, start, end time.Time) (Result, error)
}

// QuerierBuilder builds a Querier with the given params.
type QuerierBuilder interface {
	Build(params QueryParams) Querier
}

// QueryParams configures request-specific options.
type QueryParams struct {
	EvaluationInterval time.Duration
	QueryParams        url.Values
	Headers            map[string]string
	Debug              bool
}

// Result represents a PromQL query response.
type Result struct {
	Data          []Metric
	SeriesFetched *int
	IsPartial     *bool
}

// Metric is a query result series.
type Metric struct {
	Labels     []prompb.Label
	Timestamps []int64
	Values     []float64
}

// HTTPConfig configures the Prometheus HTTP client.
type HTTPConfig struct {
	URL               string
	Timeout           time.Duration
	DisablePathAppend bool
	DisableStepParam  bool
	Headers           map[string]string
	Params            url.Values
	Transport         *http.Transport
	UserAgent         string
	Logger            Logger
}

// HTTPClient implements QuerierBuilder and Querier against the Prometheus HTTP API.
type HTTPClient struct {
	baseURL            *url.URL
	queryURL           *url.URL
	queryRangeURL      *url.URL
	client             *http.Client
	disableStepParam   bool
	defaultHeaders     map[string]string
	defaultParams      url.Values
	evaluationInterval time.Duration
	debug              bool
	logger             Logger
}

// NewHTTPClient creates a new Prometheus HTTP client.
func NewHTTPClient(cfg HTTPConfig) (*HTTPClient, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("url is required")
	}
	parsed, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid url %q: %w", cfg.URL, err)
	}

	queryURL, err := withPath(parsed, cfg.DisablePathAppend, "/api/v1/query")
	if err != nil {
		return nil, err
	}
	rangeURL, err := withPath(parsed, cfg.DisablePathAppend, "/api/v1/query_range")
	if err != nil {
		return nil, err
	}

	transport := cfg.Transport
	if transport == nil {
		transport = &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     90 * time.Second,
		}
	}
	client := &http.Client{Transport: transport}
	if cfg.Timeout > 0 {
		client.Timeout = cfg.Timeout
	}

	headers := map[string]string{}
	for k, v := range cfg.Headers {
		headers[k] = v
	}
	if cfg.UserAgent != "" {
		headers["User-Agent"] = cfg.UserAgent
	}

	params := url.Values{}
	for k, v := range cfg.Params {
		params[k] = append([]string(nil), v...)
	}

	return &HTTPClient{
		baseURL:            parsed,
		queryURL:           queryURL,
		queryRangeURL:      rangeURL,
		client:             client,
		disableStepParam:   cfg.DisableStepParam,
		defaultHeaders:     headers,
		defaultParams:      params,
		evaluationInterval: 0,
		logger:             ensureLogger(cfg.Logger),
	}, nil
}

// Build implements QuerierBuilder.
func (c *HTTPClient) Build(params QueryParams) Querier {
	clone := *c
	clone.defaultHeaders = copyStringMap(c.defaultHeaders)
	clone.defaultParams = copyValues(c.defaultParams)
	clone.evaluationInterval = params.EvaluationInterval
	clone.debug = params.Debug
	clone.logger = ensureLogger(c.logger)

	for k, v := range params.Headers {
		clone.defaultHeaders[k] = v
	}

	for k, vals := range params.QueryParams {
		clone.defaultParams.Del(k)
		for _, v := range vals {
			clone.defaultParams.Add(k, v)
		}
	}

	return &clone
}

// Query executes an instant query.
func (c *HTTPClient) Query(ctx context.Context, query string, ts time.Time) (Result, error) {
	req, err := c.newQueryRequest(ctx, c.queryURL, query, ts)
	if err != nil {
		return Result{}, err
	}
	
	resp, err := c.do(req)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()

	return parseInstantResponse(resp)
}

// QueryRange executes a range query.
func (c *HTTPClient) QueryRange(ctx context.Context, query string, start, end time.Time) (Result, error) {
	if start.IsZero() || end.IsZero() {
		return Result{}, fmt.Errorf("start and end must be set")
	}
	
	req, err := c.newRangeRequest(ctx, c.queryRangeURL, query, start, end)
	if err != nil {
		return Result{}, err
	}
	
	resp, err := c.do(req)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()

	return parseRangeResponse(resp)
}

func (c *HTTPClient) newQueryRequest(ctx context.Context, base *url.URL, query string, ts time.Time) (*http.Request, error) {
	u := cloneURL(base)

	q := copyValues(c.defaultParams)
	q.Set("query", query)
	q.Set("time", formatTimestamp(ts))
	if !c.disableStepParam && c.evaluationInterval > 0 {
		q.Set("step", formatDurationSeconds(c.evaluationInterval))
	}

	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	for k, v := range c.defaultHeaders {
		req.Header.Set(k, v)
	}

	return req, nil
}

func (c *HTTPClient) newRangeRequest(ctx context.Context, base *url.URL, query string, start, end time.Time) (*http.Request, error) {
	u := cloneURL(base)

	q := copyValues(c.defaultParams)
	q.Set("query", query)
	q.Set("start", formatTimestamp(start))
	q.Set("end", formatTimestamp(end))

	step := c.evaluationInterval
	if step <= 0 {
		step = time.Minute
	}

	q.Set("step", formatDurationSeconds(step))
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	for k, v := range c.defaultHeaders {
		req.Header.Set(k, v)
	}
	
	return req, nil
}

func (c *HTTPClient) do(req *http.Request) (*http.Response, error) {
	if c.debug {
		c.logger.Debugf("promql request %s", req.URL.Redacted())
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("promql request failed: status=%s body=%q", resp.Status, body)
	}

	return resp, nil
}

func withPath(base *url.URL, disableAppend bool, suffix string) (*url.URL, error) {
	u := cloneURL(base)
	if disableAppend {
		return u, nil
	}

	if strings.HasSuffix(u.Path, suffix) {
		return u, nil
	}

	u.Path = strings.TrimSuffix(u.Path, "/")
	u.Path = path.Join(u.Path, suffix)

	return u, nil
}

func cloneURL(u *url.URL) *url.URL {
	clone := *u
	return &clone
}

func copyValues(v url.Values) url.Values {
	if v == nil {
		return url.Values{}
	}

	out := make(url.Values, len(v))
	for k, vals := range v {
		out[k] = append([]string(nil), vals...)
	}

	return out
}

func copyStringMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}

	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}

	return out
}

func formatTimestamp(ts time.Time) string {
	return strconv.FormatFloat(float64(ts.UnixNano())/1e9, 'f', -1, 64)
}

func formatDurationSeconds(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64)
}

type promResponse struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
	Data      struct {
		ResultType string          `json:"resultType"`
		Result     json.RawMessage `json:"result"`
	} `json:"data"`
	Stats struct {
		SeriesFetched *string `json:"seriesFetched,omitempty"`
	} `json:"stats,omitempty"`
	IsPartial *bool `json:"isPartial,omitempty"`
}

func parsePromResponse(resp *http.Response) (*promResponse, error) {
	var r promResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if r.Status == "error" {
		return nil, fmt.Errorf("response error %q: %s", r.ErrorType, r.Error)
	}

	if r.Status != "success" {
		return nil, fmt.Errorf("unknown response status %q", r.Status)
	}

	return &r, nil
}

func parseInstantResponse(resp *http.Response) (Result, error) {
	r, err := parsePromResponse(resp)
	if err != nil {
		return Result{}, err
	}

	var metrics []Metric
	switch r.Data.ResultType {
	case "vector":
		var items []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]any            `json:"value"`
		}
		if err := json.Unmarshal(r.Data.Result, &items); err != nil {
			return Result{}, err
		}
		metrics, err = parseVector(items)
	case "scalar":
		var scalar [2]any
		if err := json.Unmarshal(r.Data.Result, &scalar); err != nil {
			return Result{}, err
		}
		metrics, err = parseScalar(scalar)
	default:
		return Result{}, fmt.Errorf("unknown result type %q", r.Data.ResultType)
	}

	if err != nil {
		return Result{}, err
	}

	return Result{
		Data:          metrics,
		SeriesFetched: parseSeriesFetched(r.Stats.SeriesFetched),
		IsPartial:     r.IsPartial,
	}, nil
}

func parseRangeResponse(resp *http.Response) (Result, error) {
	r, err := parsePromResponse(resp)
	if err != nil {
		return Result{}, err
	}

	if r.Data.ResultType != "matrix" {
		return Result{}, fmt.Errorf("unexpected result type %q for range query", r.Data.ResultType)
	}

	var items []struct {
		Metric map[string]string `json:"metric"`
		Values [][2]any          `json:"values"`
	}

	if err := json.Unmarshal(r.Data.Result, &items); err != nil {
		return Result{}, err
	}

	metrics := make([]Metric, 0, len(items))
	for _, item := range items {
		lbls := labelsFromMap(item.Metric)
		var ts []int64
		var vs []float64
		for _, pair := range item.Values {
			t, v, err := parseSample(pair)
			if err != nil {
				return Result{}, err
			}
			ts = append(ts, t)
			vs = append(vs, v)
		}
		metrics = append(metrics, Metric{
			Labels:     lbls,
			Timestamps: ts,
			Values:     vs,
		})
	}

	return Result{
		Data:          metrics,
		SeriesFetched: parseSeriesFetched(r.Stats.SeriesFetched),
		IsPartial:     r.IsPartial,
	}, nil
}

func parseVector(items []struct {
	Metric map[string]string `json:"metric"`
	Value  [2]any            `json:"value"`
}) ([]Metric, error) {
	metrics := make([]Metric, 0, len(items))
	for _, item := range items {
		t, v, err := parseSample(item.Value)
		if err != nil {
			return nil, err
		}
		metrics = append(metrics, Metric{
			Labels:     labelsFromMap(item.Metric),
			Timestamps: []int64{t},
			Values:     []float64{v},
		})
	}

	return metrics, nil
}

func parseScalar(pair [2]any) ([]Metric, error) {
	t, v, err := parseSample(pair)
	if err != nil {
		return nil, err
	}

	return []Metric{{
		Labels:     nil,
		Timestamps: []int64{t},
		Values:     []float64{v},
	}}, nil
}

func parseSample(pair [2]any) (int64, float64, error) {
	ts, ok := toFloat(pair[0])
	if !ok {
		return 0, 0, fmt.Errorf("invalid timestamp value %v", pair[0])
	}

	valStr, ok := pair[1].(string)
	if !ok {
		return 0, 0, fmt.Errorf("invalid sample value %v", pair[1])
	}

	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid sample value %q: %w", valStr, err)
	}

	return int64(ts * 1000), val, nil
}

func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case json.Number:
		f, err := t.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

func labelsFromMap(m map[string]string) []prompb.Label {
	if len(m) == 0 {
		return nil
	}

	lbls := make([]prompb.Label, 0, len(m))
	for k, v := range m {
		lbls = append(lbls, prompb.Label{Name: k, Value: v})
	}
	sort.Slice(lbls, func(i, j int) bool { return lbls[i].Name < lbls[j].Name })

	return lbls
}

func parseSeriesFetched(v *string) *int {
	if v == nil {
		return nil
	}

	n, err := strconv.Atoi(*v)
	if err != nil {
		return nil
	}
	
	return &n
}
