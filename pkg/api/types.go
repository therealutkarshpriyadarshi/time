package api

import (
	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

// WriteRequest represents a Prometheus-compatible remote write request.
type WriteRequest struct {
	Timeseries []TimeSeries `json:"timeseries"`
}

// TimeSeries represents a series with labels and samples.
type TimeSeries struct {
	Labels  []Label  `json:"labels"`
	Samples []Sample `json:"samples"`
}

// Label represents a label name-value pair.
type Label struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Sample represents a timestamp-value pair.
type Sample struct {
	Timestamp int64   `json:"timestamp"` // Unix milliseconds
	Value     float64 `json:"value"`
}

// QueryRequest represents a query request.
type QueryRequest struct {
	Query string `json:"query"` // Label matchers string, e.g., {__name__="cpu_usage",host="server1"}
	Time  int64  `json:"time"`  // Unix timestamp in milliseconds (for instant queries)
}

// QueryRangeRequest represents a range query request.
type QueryRangeRequest struct {
	Query string `json:"query"` // Label matchers string
	Start int64  `json:"start"` // Start time in Unix milliseconds
	End   int64  `json:"end"`   // End time in Unix milliseconds
	Step  int64  `json:"step"`  // Step duration in milliseconds
}

// QueryResponse represents the response to a query.
type QueryResponse struct {
	Status string     `json:"status"`
	Data   *QueryData `json:"data,omitempty"`
	Error  string     `json:"error,omitempty"`
}

// QueryData contains the query result data.
type QueryData struct {
	ResultType string        `json:"resultType"` // "matrix" or "vector"
	Result     []QueryResult `json:"result"`
}

// QueryResult represents a single time series result.
type QueryResult struct {
	Metric map[string]string `json:"metric"`
	Values [][]interface{}   `json:"values,omitempty"` // For range queries: [[timestamp, "value"], ...]
	Value  []interface{}     `json:"value,omitempty"`  // For instant queries: [timestamp, "value"]
}

// LabelsResponse represents the response to a labels query.
type LabelsResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data,omitempty"`
	Error  string   `json:"error,omitempty"`
}

// LabelValuesResponse represents the response to a label values query.
type LabelValuesResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data,omitempty"`
	Error  string   `json:"error,omitempty"`
}

// SeriesResponse represents the response to a series query.
type SeriesResponse struct {
	Status string              `json:"status"`
	Data   []map[string]string `json:"data,omitempty"`
	Error  string              `json:"error,omitempty"`
}

// StatusResponse represents the response to a status/tsdb query.
type StatusResponse struct {
	Status string      `json:"status"`
	Data   *StatusData `json:"data,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// StatusData contains TSDB status information.
type StatusData struct {
	TotalSamples       int64 `json:"totalSamples"`
	TotalSeries        int64 `json:"totalSeries"`
	FlushCount         int64 `json:"flushCount"`
	LastFlushTime      int64 `json:"lastFlushTime"`
	WALSize            int64 `json:"walSize"`
	ActiveMemTableSize int64 `json:"activeMemTableSize"`
}

// HealthResponse represents the response to a health check.
type HealthResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// ToSeriesSamples converts API types to internal series and samples.
func (ts *TimeSeries) ToSeriesSamples() (*series.Series, []series.Sample) {
	// Convert labels
	labels := make(map[string]string, len(ts.Labels))
	for _, l := range ts.Labels {
		labels[l.Name] = l.Value
	}

	// Create series
	s := series.NewSeries(labels)

	// Convert samples
	samples := make([]series.Sample, len(ts.Samples))
	for i, sample := range ts.Samples {
		samples[i] = series.Sample{
			Timestamp: sample.Timestamp,
			Value:     sample.Value,
		}
	}

	return s, samples
}
