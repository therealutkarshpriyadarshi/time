package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/therealutkarshpriyadarshi/time/pkg/api"
)

// Client is a client for the TSDB HTTP API.
type Client struct {
	baseURL    string
	httpClient *http.Client
	userAgent  string
}

// Option is a function that configures a Client.
type Option func(*Client)

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) {
		c.httpClient = client
	}
}

// WithUserAgent sets a custom user agent.
func WithUserAgent(ua string) Option {
	return func(c *Client) {
		c.userAgent = ua
	}
}

// WithTimeout sets the HTTP client timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) {
		c.httpClient.Timeout = timeout
	}
}

// NewClient creates a new TSDB client.
func NewClient(addr string, opts ...Option) *Client {
	c := &Client{
		baseURL: addr,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		userAgent: "tsdb-go-client/1.0",
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// Metric represents a time-series metric with labels and a value.
type Metric struct {
	Labels    map[string]string
	Timestamp time.Time
	Value     float64
}

// QueryResult represents the result of a query.
type QueryResult struct {
	Labels  map[string]string
	Samples []Sample
}

// Sample represents a single data point.
type Sample struct {
	Timestamp time.Time
	Value     float64
}

// Write writes metrics to the TSDB.
func (c *Client) Write(ctx context.Context, metrics []Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	// Convert to API request format
	req := api.WriteRequest{
		Timeseries: make([]api.TimeSeries, 0, len(metrics)),
	}

	// Group metrics by labels
	grouped := make(map[string]*api.TimeSeries)

	for _, m := range metrics {
		// Create a key from labels
		key := labelsKey(m.Labels)

		ts, ok := grouped[key]
		if !ok {
			// Create new time series
			labels := make([]api.Label, 0, len(m.Labels))
			for name, value := range m.Labels {
				labels = append(labels, api.Label{
					Name:  name,
					Value: value,
				})
			}

			ts = &api.TimeSeries{
				Labels:  labels,
				Samples: []api.Sample{},
			}
			grouped[key] = ts
		}

		// Add sample
		ts.Samples = append(ts.Samples, api.Sample{
			Timestamp: m.Timestamp.UnixMilli(),
			Value:     m.Value,
		})
	}

	// Convert map to slice
	for _, ts := range grouped {
		req.Timeseries = append(req.Timeseries, *ts)
	}

	// Marshal request
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	// Send request
	url := c.baseURL + "/api/v1/write"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", c.userAgent)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// Query executes an instant query.
func (c *Client) Query(ctx context.Context, query string, ts time.Time) ([]QueryResult, error) {
	params := url.Values{}
	params.Set("query", query)
	params.Set("time", strconv.FormatInt(ts.UnixMilli(), 10))

	url := c.baseURL + "/api/v1/query?" + params.Encode()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("User-Agent", c.userAgent)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var apiResp api.QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if apiResp.Status != "success" {
		return nil, fmt.Errorf("query failed: %s", apiResp.Error)
	}

	// Convert to client format
	results := make([]QueryResult, 0, len(apiResp.Data.Result))
	for _, r := range apiResp.Data.Result {
		result := QueryResult{
			Labels: r.Metric,
		}

		if r.Value != nil && len(r.Value) == 2 {
			timestamp := int64(r.Value[0].(float64))
			value, _ := strconv.ParseFloat(r.Value[1].(string), 64)

			result.Samples = []Sample{
				{
					Timestamp: time.UnixMilli(timestamp),
					Value:     value,
				},
			}
		}

		results = append(results, result)
	}

	return results, nil
}

// QueryRange executes a range query.
func (c *Client) QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration) ([]QueryResult, error) {
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", strconv.FormatInt(start.UnixMilli(), 10))
	params.Set("end", strconv.FormatInt(end.UnixMilli(), 10))
	params.Set("step", strconv.FormatInt(step.Milliseconds(), 10))

	url := c.baseURL + "/api/v1/query_range?" + params.Encode()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("User-Agent", c.userAgent)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var apiResp api.QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if apiResp.Status != "success" {
		return nil, fmt.Errorf("query failed: %s", apiResp.Error)
	}

	// Convert to client format
	results := make([]QueryResult, 0, len(apiResp.Data.Result))
	for _, r := range apiResp.Data.Result {
		result := QueryResult{
			Labels:  r.Metric,
			Samples: make([]Sample, 0, len(r.Values)),
		}

		for _, v := range r.Values {
			if len(v) == 2 {
				timestamp := int64(v[0].(float64))
				value, _ := strconv.ParseFloat(v[1].(string), 64)

				result.Samples = append(result.Samples, Sample{
					Timestamp: time.UnixMilli(timestamp),
					Value:     value,
				})
			}
		}

		results = append(results, result)
	}

	return results, nil
}

// Labels returns all unique label names.
func (c *Client) Labels(ctx context.Context) ([]string, error) {
	url := c.baseURL + "/api/v1/labels"

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("User-Agent", c.userAgent)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var apiResp api.LabelsResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if apiResp.Status != "success" {
		return nil, fmt.Errorf("request failed: %s", apiResp.Error)
	}

	return apiResp.Data, nil
}

// LabelValues returns all values for a specific label.
func (c *Client) LabelValues(ctx context.Context, labelName string) ([]string, error) {
	url := fmt.Sprintf("%s/api/v1/label/%s/values", c.baseURL, labelName)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("User-Agent", c.userAgent)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var apiResp api.LabelValuesResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if apiResp.Status != "success" {
		return nil, fmt.Errorf("request failed: %s", apiResp.Error)
	}

	return apiResp.Data, nil
}

// Health checks if the TSDB is healthy.
func (c *Client) Health(ctx context.Context) (bool, error) {
	url := c.baseURL + "/-/healthy"

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("User-Agent", c.userAgent)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return false, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK, nil
}

// labelsKey creates a unique key from labels for grouping.
func labelsKey(labels map[string]string) string {
	key := ""
	for name, value := range labels {
		key += name + "=" + value + ","
	}
	return key
}
