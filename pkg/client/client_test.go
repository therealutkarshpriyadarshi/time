package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/therealutkarshpriyadarshi/time/pkg/api"
	"github.com/therealutkarshpriyadarshi/time/pkg/storage"
)

func setupTestServerWithClient(t *testing.T) (*Client, *storage.TSDB, func()) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "tsdb-client-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Create TSDB
	opts := storage.DefaultOptions(tmpDir)
	opts.EnableCompaction = false
	opts.EnableRetention = false

	db, err := storage.Open(opts)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to open TSDB: %v", err)
	}

	// Create API server
	server := api.NewServer(db, ":0")

	// Create test HTTP server
	httpServer := httptest.NewServer(server.mux)

	// Create client
	client := NewClient(httpServer.URL)

	cleanup := func() {
		httpServer.Close()
		db.Close()
		os.RemoveAll(tmpDir)
	}

	return client, db, cleanup
}

func TestClientWrite(t *testing.T) {
	client, _, cleanup := setupTestServerWithClient(t)
	defer cleanup()

	ctx := context.Background()

	tests := []struct {
		name    string
		metrics []Metric
		wantErr bool
	}{
		{
			name: "write single metric",
			metrics: []Metric{
				{
					Labels: map[string]string{
						"__name__": "cpu_usage",
						"host":     "server1",
					},
					Timestamp: time.Now(),
					Value:     0.75,
				},
			},
			wantErr: false,
		},
		{
			name: "write multiple metrics",
			metrics: []Metric{
				{
					Labels: map[string]string{
						"__name__": "memory_usage",
						"host":     "server1",
					},
					Timestamp: time.Now(),
					Value:     1024.0,
				},
				{
					Labels: map[string]string{
						"__name__": "disk_usage",
						"host":     "server2",
					},
					Timestamp: time.Now(),
					Value:     2048.0,
				},
			},
			wantErr: false,
		},
		{
			name:    "write empty metrics",
			metrics: []Metric{},
			wantErr: false, // Should succeed but do nothing
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.Write(ctx, tt.metrics)
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClientQuery(t *testing.T) {
	client, _, cleanup := setupTestServerWithClient(t)
	defer cleanup()

	ctx := context.Background()

	// Write test data
	now := time.Now()
	metrics := []Metric{
		{
			Labels: map[string]string{
				"__name__": "test_metric",
				"host":     "server1",
			},
			Timestamp: now,
			Value:     1.0,
		},
	}

	if err := client.Write(ctx, metrics); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Query the data
	results, err := client.Query(ctx, `{__name__="test_metric",host="server1"}`, now)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if len(results) == 0 {
		t.Error("Expected results, got none")
	}

	// Verify result
	if len(results) > 0 {
		result := results[0]
		if result.Labels["__name__"] != "test_metric" {
			t.Errorf("Expected __name__=test_metric, got %s", result.Labels["__name__"])
		}
		if result.Labels["host"] != "server1" {
			t.Errorf("Expected host=server1, got %s", result.Labels["host"])
		}
	}
}

func TestClientQueryRange(t *testing.T) {
	client, _, cleanup := setupTestServerWithClient(t)
	defer cleanup()

	ctx := context.Background()

	// Write test data with multiple timestamps
	now := time.Now()
	metrics := []Metric{
		{
			Labels: map[string]string{
				"__name__": "range_metric",
				"host":     "server1",
			},
			Timestamp: now.Add(-2 * time.Minute),
			Value:     1.0,
		},
		{
			Labels: map[string]string{
				"__name__": "range_metric",
				"host":     "server1",
			},
			Timestamp: now.Add(-1 * time.Minute),
			Value:     2.0,
		},
		{
			Labels: map[string]string{
				"__name__": "range_metric",
				"host":     "server1",
			},
			Timestamp: now,
			Value:     3.0,
		},
	}

	if err := client.Write(ctx, metrics); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Query range
	start := now.Add(-3 * time.Minute)
	end := now.Add(1 * time.Minute)
	step := 1 * time.Minute

	results, err := client.QueryRange(ctx, `{__name__="range_metric",host="server1"}`, start, end, step)
	if err != nil {
		t.Fatalf("QueryRange() error = %v", err)
	}

	if len(results) == 0 {
		t.Error("Expected results, got none")
	}

	// Verify results
	if len(results) > 0 {
		result := results[0]
		if result.Labels["__name__"] != "range_metric" {
			t.Errorf("Expected __name__=range_metric, got %s", result.Labels["__name__"])
		}

		if len(result.Samples) == 0 {
			t.Error("Expected samples, got none")
		}
	}
}

func TestClientLabels(t *testing.T) {
	client, _, cleanup := setupTestServerWithClient(t)
	defer cleanup()

	ctx := context.Background()

	// Write test data
	metrics := []Metric{
		{
			Labels: map[string]string{
				"__name__": "metric1",
				"host":     "server1",
				"region":   "us-west",
			},
			Timestamp: time.Now(),
			Value:     1.0,
		},
	}

	if err := client.Write(ctx, metrics); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Get labels
	labels, err := client.Labels(ctx)
	if err != nil {
		t.Fatalf("Labels() error = %v", err)
	}

	if len(labels) == 0 {
		t.Error("Expected labels, got none")
	}

	// Verify expected labels are present
	expectedLabels := map[string]bool{
		"__name__": false,
		"host":     false,
		"region":   false,
	}

	for _, label := range labels {
		if _, ok := expectedLabels[label]; ok {
			expectedLabels[label] = true
		}
	}

	for label, found := range expectedLabels {
		if !found {
			t.Errorf("Expected label %s not found", label)
		}
	}
}

func TestClientLabelValues(t *testing.T) {
	client, _, cleanup := setupTestServerWithClient(t)
	defer cleanup()

	ctx := context.Background()

	// Write test data with multiple hosts
	metrics := []Metric{
		{
			Labels: map[string]string{
				"__name__": "metric1",
				"host":     "server1",
			},
			Timestamp: time.Now(),
			Value:     1.0,
		},
		{
			Labels: map[string]string{
				"__name__": "metric1",
				"host":     "server2",
			},
			Timestamp: time.Now(),
			Value:     2.0,
		},
	}

	if err := client.Write(ctx, metrics); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Get label values for "host"
	values, err := client.LabelValues(ctx, "host")
	if err != nil {
		t.Fatalf("LabelValues() error = %v", err)
	}

	if len(values) != 2 {
		t.Errorf("Expected 2 label values, got %d", len(values))
	}

	// Verify both values are present
	valueMap := make(map[string]bool)
	for _, v := range values {
		valueMap[v] = true
	}

	if !valueMap["server1"] || !valueMap["server2"] {
		t.Error("Expected server1 and server2 in label values")
	}
}

func TestClientHealth(t *testing.T) {
	client, _, cleanup := setupTestServerWithClient(t)
	defer cleanup()

	ctx := context.Background()

	healthy, err := client.Health(ctx)
	if err != nil {
		t.Fatalf("Health() error = %v", err)
	}

	if !healthy {
		t.Error("Expected healthy=true, got false")
	}
}

func TestClientOptions(t *testing.T) {
	// Test WithTimeout option
	client := NewClient("http://localhost:8080", WithTimeout(5*time.Second))
	if client.httpClient.Timeout != 5*time.Second {
		t.Errorf("Expected timeout=5s, got %v", client.httpClient.Timeout)
	}

	// Test WithUserAgent option
	client = NewClient("http://localhost:8080", WithUserAgent("custom-agent/1.0"))
	if client.userAgent != "custom-agent/1.0" {
		t.Errorf("Expected user agent=custom-agent/1.0, got %s", client.userAgent)
	}

	// Test WithHTTPClient option
	customHTTPClient := &http.Client{Timeout: 10 * time.Second}
	client = NewClient("http://localhost:8080", WithHTTPClient(customHTTPClient))
	if client.httpClient != customHTTPClient {
		t.Error("Expected custom HTTP client to be set")
	}
}

func TestClientWriteGrouping(t *testing.T) {
	// Test that metrics with the same labels are grouped together
	client, _, cleanup := setupTestServerWithClient(t)
	defer cleanup()

	ctx := context.Background()

	// Write metrics with same labels but different timestamps
	now := time.Now()
	metrics := []Metric{
		{
			Labels: map[string]string{
				"__name__": "grouped_metric",
				"host":     "server1",
			},
			Timestamp: now,
			Value:     1.0,
		},
		{
			Labels: map[string]string{
				"__name__": "grouped_metric",
				"host":     "server1",
			},
			Timestamp: now.Add(1 * time.Second),
			Value:     2.0,
		},
	}

	err := client.Write(ctx, metrics)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Verify both samples were written
	results, err := client.QueryRange(ctx,
		`{__name__="grouped_metric",host="server1"}`,
		now.Add(-1*time.Minute),
		now.Add(2*time.Minute),
		1*time.Second,
	)

	if err != nil {
		t.Fatalf("QueryRange() error = %v", err)
	}

	if len(results) == 0 {
		t.Fatal("Expected results, got none")
	}

	// Should have at least 2 samples
	if len(results[0].Samples) < 2 {
		t.Errorf("Expected at least 2 samples, got %d", len(results[0].Samples))
	}
}
