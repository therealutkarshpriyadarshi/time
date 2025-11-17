package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/therealutkarshpriyadarshi/time/pkg/storage"
)

func TestMain(m *testing.M) {
	// Run tests
	code := m.Run()
	os.Exit(code)
}

func setupTestServer(t *testing.T) (*Server, *storage.TSDB, func()) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "tsdb-api-test-*")
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

	// Create server
	server := NewServer(db, ":0")

	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}

	return server, db, cleanup
}

func TestHandleWrite(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	tests := []struct {
		name       string
		request    WriteRequest
		wantStatus int
	}{
		{
			name: "valid write request",
			request: WriteRequest{
				Timeseries: []TimeSeries{
					{
						Labels: []Label{
							{Name: "__name__", Value: "cpu_usage"},
							{Name: "host", Value: "server1"},
						},
						Samples: []Sample{
							{Timestamp: 1000, Value: 0.75},
							{Timestamp: 2000, Value: 0.82},
						},
					},
				},
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "multiple series",
			request: WriteRequest{
				Timeseries: []TimeSeries{
					{
						Labels: []Label{
							{Name: "__name__", Value: "memory_usage"},
							{Name: "host", Value: "server1"},
						},
						Samples: []Sample{
							{Timestamp: 1000, Value: 1024.0},
						},
					},
					{
						Labels: []Label{
							{Name: "__name__", Value: "disk_usage"},
							{Name: "host", Value: "server2"},
						},
						Samples: []Sample{
							{Timestamp: 1000, Value: 2048.0},
						},
					},
				},
			},
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := json.Marshal(tt.request)
			if err != nil {
				t.Fatalf("Failed to marshal request: %v", err)
			}

			req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")

			w := httptest.NewRecorder()
			server.handleWrite(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("handleWrite() status = %d, want %d", w.Code, tt.wantStatus)
			}
		})
	}
}

func TestHandleWriteInvalidMethod(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/write", nil)
	w := httptest.NewRecorder()

	server.handleWrite(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("handleWrite() with GET status = %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleQueryRange(t *testing.T) {
	server, db, cleanup := setupTestServer(t)
	defer cleanup()

	// Insert test data
	writeReq := WriteRequest{
		Timeseries: []TimeSeries{
			{
				Labels: []Label{
					{Name: "__name__", Value: "test_metric"},
					{Name: "host", Value: "server1"},
				},
				Samples: []Sample{
					{Timestamp: 1000, Value: 1.0},
					{Timestamp: 2000, Value: 2.0},
					{Timestamp: 3000, Value: 3.0},
				},
			},
		},
	}

	for _, ts := range writeReq.Timeseries {
		s, samples := ts.ToSeriesSamples()
		if err := db.Insert(s, samples); err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Wait a bit for data to be available
	time.Sleep(100 * time.Millisecond)

	tests := []struct {
		name       string
		query      string
		start      string
		end        string
		step       string
		wantStatus int
	}{
		{
			name:       "valid query range",
			query:      `{__name__="test_metric",host="server1"}`,
			start:      "0",
			end:        "5000",
			step:       "1000",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing query parameter",
			query:      "",
			start:      "0",
			end:        "5000",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing start parameter",
			query:      `{__name__="test_metric"}`,
			start:      "",
			end:        "5000",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid start parameter",
			query:      `{__name__="test_metric"}`,
			start:      "invalid",
			end:        "5000",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := "/api/v1/query_range?query=" + tt.query + "&start=" + tt.start + "&end=" + tt.end
			if tt.step != "" {
				url += "&step=" + tt.step
			}

			req := httptest.NewRequest(http.MethodGet, url, nil)
			w := httptest.NewRecorder()

			server.handleQueryRange(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("handleQueryRange() status = %d, want %d, body: %s", w.Code, tt.wantStatus, w.Body.String())
			}

			if w.Code == http.StatusOK {
				var resp QueryResponse
				if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
					t.Fatalf("Failed to decode response: %v", err)
				}

				if resp.Status != "success" {
					t.Errorf("Response status = %s, want success", resp.Status)
				}

				if resp.Data == nil {
					t.Error("Response data is nil")
				}

				if resp.Data.ResultType != "matrix" {
					t.Errorf("Result type = %s, want matrix", resp.Data.ResultType)
				}
			}
		})
	}
}

func TestHandleLabels(t *testing.T) {
	server, db, cleanup := setupTestServer(t)
	defer cleanup()

	// Insert test data with various labels
	writeReq := WriteRequest{
		Timeseries: []TimeSeries{
			{
				Labels: []Label{
					{Name: "__name__", Value: "metric1"},
					{Name: "host", Value: "server1"},
					{Name: "region", Value: "us-west"},
				},
				Samples: []Sample{
					{Timestamp: 1000, Value: 1.0},
				},
			},
		},
	}

	for _, ts := range writeReq.Timeseries {
		s, samples := ts.ToSeriesSamples()
		if err := db.Insert(s, samples); err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/labels", nil)
	w := httptest.NewRecorder()

	server.handleLabels(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("handleLabels() status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp LabelsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.Status != "success" {
		t.Errorf("Response status = %s, want success", resp.Status)
	}

	if len(resp.Data) == 0 {
		t.Error("Expected labels, got none")
	}

	// Check that expected labels are present
	expectedLabels := map[string]bool{
		"__name__": false,
		"host":     false,
		"region":   false,
	}

	for _, label := range resp.Data {
		if _, ok := expectedLabels[label]; ok {
			expectedLabels[label] = true
		}
	}

	for label, found := range expectedLabels {
		if !found {
			t.Errorf("Expected label %s not found in response", label)
		}
	}
}

func TestHandleLabelValues(t *testing.T) {
	server, db, cleanup := setupTestServer(t)
	defer cleanup()

	// Insert test data
	writeReq := WriteRequest{
		Timeseries: []TimeSeries{
			{
				Labels: []Label{
					{Name: "__name__", Value: "metric1"},
					{Name: "host", Value: "server1"},
				},
				Samples: []Sample{{Timestamp: 1000, Value: 1.0}},
			},
			{
				Labels: []Label{
					{Name: "__name__", Value: "metric1"},
					{Name: "host", Value: "server2"},
				},
				Samples: []Sample{{Timestamp: 1000, Value: 2.0}},
			},
		},
	}

	for _, ts := range writeReq.Timeseries {
		s, samples := ts.ToSeriesSamples()
		if err := db.Insert(s, samples); err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/label/host/values", nil)
	w := httptest.NewRecorder()

	server.handleLabelValues(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("handleLabelValues() status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp LabelValuesResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.Status != "success" {
		t.Errorf("Response status = %s, want success", resp.Status)
	}

	if len(resp.Data) != 2 {
		t.Errorf("Expected 2 label values, got %d", len(resp.Data))
	}

	// Check that both values are present
	valueMap := make(map[string]bool)
	for _, v := range resp.Data {
		valueMap[v] = true
	}

	if !valueMap["server1"] || !valueMap["server2"] {
		t.Error("Expected server1 and server2 in label values")
	}
}

func TestHandleStatus(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/status/tsdb", nil)
	w := httptest.NewRecorder()

	server.handleStatus(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("handleStatus() status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp StatusResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.Status != "success" {
		t.Errorf("Response status = %s, want success", resp.Status)
	}

	if resp.Data == nil {
		t.Error("Response data is nil")
	}
}

func TestHandleHealthy(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/-/healthy", nil)
	w := httptest.NewRecorder()

	server.handleHealthy(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("handleHealthy() status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp HealthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.Status != "healthy" {
		t.Errorf("Response status = %s, want healthy", resp.Status)
	}
}

func TestHandleReady(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/-/ready", nil)
	w := httptest.NewRecorder()

	server.handleReady(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("handleReady() status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp HealthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.Status != "ready" {
		t.Errorf("Response status = %s, want ready", resp.Status)
	}
}

func TestParseMatchers(t *testing.T) {
	tests := []struct {
		name        string
		queryStr    string
		wantErr     bool
		matchersLen int
	}{
		{
			name:        "single equal matcher",
			queryStr:    `{__name__="cpu_usage"}`,
			wantErr:     false,
			matchersLen: 1,
		},
		{
			name:        "multiple matchers",
			queryStr:    `{__name__="cpu_usage",host="server1"}`,
			wantErr:     false,
			matchersLen: 2,
		},
		{
			name:        "empty matcher",
			queryStr:    `{}`,
			wantErr:     false,
			matchersLen: 0,
		},
		{
			name:        "not equal matcher",
			queryStr:    `{host!="server1"}`,
			wantErr:     false,
			matchersLen: 1,
		},
		{
			name:     "invalid format - no braces",
			queryStr: `cpu_usage`,
			wantErr:  true,
		},
		{
			name:     "invalid format - missing closing brace",
			queryStr: `{__name__="cpu_usage"`,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matchers, err := parseMatchers(tt.queryStr)

			if tt.wantErr {
				if err == nil {
					t.Error("parseMatchers() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("parseMatchers() unexpected error: %v", err)
				return
			}

			if len(matchers) != tt.matchersLen {
				t.Errorf("parseMatchers() matchers length = %d, want %d", len(matchers), tt.matchersLen)
			}
		})
	}
}

func TestServerShutdown(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Start server in a goroutine
	go func() {
		// This will fail with "bind: address already in use" or similar
		// but we just want to test the shutdown mechanism
		server.Start()
	}()

	time.Sleep(100 * time.Millisecond)

	// Test shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := server.Shutdown(ctx)
	if err != nil {
		t.Logf("Shutdown returned error (expected for test): %v", err)
	}
}
