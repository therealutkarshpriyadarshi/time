package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/therealutkarshpriyadarshi/time/pkg/index"
	"github.com/therealutkarshpriyadarshi/time/pkg/query"
	"github.com/therealutkarshpriyadarshi/time/pkg/storage"
)

// Server is the HTTP API server for the TSDB.
type Server struct {
	db     *storage.TSDB
	engine *query.QueryEngine
	mux    *http.ServeMux
	server *http.Server
	addr   string
}

// NewServer creates a new API server.
func NewServer(db *storage.TSDB, addr string) *Server {
	s := &Server{
		db:     db,
		engine: query.NewQueryEngine(db),
		mux:    http.NewServeMux(),
		addr:   addr,
	}

	s.registerRoutes()

	s.server = &http.Server{
		Addr:         addr,
		Handler:      s.mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	return s
}

// registerRoutes sets up all HTTP routes.
func (s *Server) registerRoutes() {
	// Write endpoint
	s.mux.HandleFunc("/api/v1/write", s.handleWrite)

	// Query endpoints
	s.mux.HandleFunc("/api/v1/query", s.handleQuery)
	s.mux.HandleFunc("/api/v1/query_range", s.handleQueryRange)

	// Metadata endpoints
	s.mux.HandleFunc("/api/v1/labels", s.handleLabels)
	s.mux.HandleFunc("/api/v1/label/", s.handleLabelValues)
	s.mux.HandleFunc("/api/v1/series", s.handleSeries)

	// Admin endpoints
	s.mux.HandleFunc("/api/v1/status/tsdb", s.handleStatus)

	// Health endpoints
	s.mux.HandleFunc("/-/healthy", s.handleHealthy)
	s.mux.HandleFunc("/-/ready", s.handleReady)
}

// Start starts the HTTP server.
func (s *Server) Start() error {
	log.Printf("Starting API server on %s", s.addr)
	return s.server.ListenAndServe()
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	log.Printf("Shutting down API server")
	return s.server.Shutdown(ctx)
}

// handleWrite handles the Prometheus remote write endpoint.
func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req WriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Insert each time series
	for _, ts := range req.Timeseries {
		s, samples := ts.ToSeriesSamples()
		if err := s.db.Insert(s, samples); err != nil {
			http.Error(w, fmt.Sprintf("Insert failed: %v", err), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleQuery handles instant query requests.
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	queryStr := r.URL.Query().Get("query")
	timeStr := r.URL.Query().Get("time")

	if queryStr == "" {
		s.writeErrorResponse(w, "query parameter is required", http.StatusBadRequest)
		return
	}

	// Parse time parameter (default to now)
	queryTime := time.Now().UnixMilli()
	if timeStr != "" {
		t, err := strconv.ParseInt(timeStr, 10, 64)
		if err != nil {
			s.writeErrorResponse(w, fmt.Sprintf("Invalid time parameter: %v", err), http.StatusBadRequest)
			return
		}
		queryTime = t
	}

	// Parse matchers from query string
	matchers, err := parseMatchers(queryStr)
	if err != nil {
		s.writeErrorResponse(w, fmt.Sprintf("Invalid query: %v", err), http.StatusBadRequest)
		return
	}

	// Execute query
	q := &query.Query{
		Matchers: matchers,
		MinTime:  queryTime,
		MaxTime:  queryTime,
		Step:     0,
	}

	results, err := s.engine.ExecQuery(q)
	if err != nil {
		s.writeErrorResponse(w, fmt.Sprintf("Query failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Convert to API response format (instant query returns single value per series)
	queryResults := make([]QueryResult, 0, len(results))
	for _, result := range results {
		// For instant query, find the sample closest to queryTime
		if len(result.Samples) > 0 {
			sample := result.Samples[len(result.Samples)-1] // Take latest sample
			queryResults = append(queryResults, QueryResult{
				Metric: result.Labels,
				Value:  []interface{}{sample.Timestamp, fmt.Sprintf("%f", sample.Value)},
			})
		}
	}

	response := QueryResponse{
		Status: "success",
		Data: &QueryData{
			ResultType: "vector",
			Result:     queryResults,
		},
	}

	s.writeJSONResponse(w, response, http.StatusOK)
}

// handleQueryRange handles range query requests.
func (s *Server) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	queryStr := r.URL.Query().Get("query")
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	stepStr := r.URL.Query().Get("step")

	if queryStr == "" || startStr == "" || endStr == "" {
		s.writeErrorResponse(w, "query, start, and end parameters are required", http.StatusBadRequest)
		return
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		s.writeErrorResponse(w, fmt.Sprintf("Invalid start parameter: %v", err), http.StatusBadRequest)
		return
	}

	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		s.writeErrorResponse(w, fmt.Sprintf("Invalid end parameter: %v", err), http.StatusBadRequest)
		return
	}

	step := int64(60000) // Default 1 minute
	if stepStr != "" {
		step, err = strconv.ParseInt(stepStr, 10, 64)
		if err != nil {
			s.writeErrorResponse(w, fmt.Sprintf("Invalid step parameter: %v", err), http.StatusBadRequest)
			return
		}
	}

	// Parse matchers from query string
	matchers, err := parseMatchers(queryStr)
	if err != nil {
		s.writeErrorResponse(w, fmt.Sprintf("Invalid query: %v", err), http.StatusBadRequest)
		return
	}

	// Execute query
	q := &query.Query{
		Matchers: matchers,
		MinTime:  start,
		MaxTime:  end,
		Step:     step,
	}

	results, err := s.engine.ExecQuery(q)
	if err != nil {
		s.writeErrorResponse(w, fmt.Sprintf("Query failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Convert to API response format
	queryResults := make([]QueryResult, 0, len(results))
	for _, result := range results {
		values := make([][]interface{}, 0, len(result.Samples))
		for _, sample := range result.Samples {
			values = append(values, []interface{}{sample.Timestamp, fmt.Sprintf("%f", sample.Value)})
		}
		queryResults = append(queryResults, QueryResult{
			Metric: result.Labels,
			Values: values,
		})
	}

	response := QueryResponse{
		Status: "success",
		Data: &QueryData{
			ResultType: "matrix",
			Result:     queryResults,
		},
	}

	s.writeJSONResponse(w, response, http.StatusOK)
}

// handleLabels returns all label names.
func (s *Server) handleLabels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	labels, err := s.db.GetAllLabels()
	if err != nil {
		s.writeErrorResponse(w, fmt.Sprintf("Failed to get labels: %v", err), http.StatusInternalServerError)
		return
	}

	response := LabelsResponse{
		Status: "success",
		Data:   labels,
	}

	s.writeJSONResponse(w, response, http.StatusOK)
}

// handleLabelValues returns all values for a specific label.
func (s *Server) handleLabelValues(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract label name from URL path
	// URL format: /api/v1/label/<name>/values
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/label/")
	labelName := strings.TrimSuffix(path, "/values")

	if labelName == "" {
		s.writeErrorResponse(w, "label name is required", http.StatusBadRequest)
		return
	}

	values, err := s.db.GetLabelValues(labelName)
	if err != nil {
		s.writeErrorResponse(w, fmt.Sprintf("Failed to get label values: %v", err), http.StatusInternalServerError)
		return
	}

	response := LabelValuesResponse{
		Status: "success",
		Data:   values,
	}

	s.writeJSONResponse(w, response, http.StatusOK)
}

// handleSeries returns all series matching the provided label matchers.
func (s *Server) handleSeries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get match[] parameters
	matches := r.URL.Query()["match[]"]
	if len(matches) == 0 {
		s.writeErrorResponse(w, "at least one match[] parameter is required", http.StatusBadRequest)
		return
	}

	allSeries := make([]map[string]string, 0)

	// For each matcher, get matching series
	for _, match := range matches {
		matchers, err := parseMatchers(match)
		if err != nil {
			s.writeErrorResponse(w, fmt.Sprintf("Invalid matcher: %v", err), http.StatusBadRequest)
			return
		}

		series, err := s.db.GetSeries(matchers)
		if err != nil {
			s.writeErrorResponse(w, fmt.Sprintf("Failed to get series: %v", err), http.StatusInternalServerError)
			return
		}

		allSeries = append(allSeries, series...)
	}

	response := SeriesResponse{
		Status: "success",
		Data:   allSeries,
	}

	s.writeJSONResponse(w, response, http.StatusOK)
}

// handleStatus returns TSDB status information.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := s.db.GetStatsSnapshot()

	response := StatusResponse{
		Status: "success",
		Data: &StatusData{
			TotalSamples:       stats.TotalSamples,
			TotalSeries:        stats.TotalSeries,
			FlushCount:         stats.FlushCount,
			LastFlushTime:      stats.LastFlushTime,
			WALSize:            stats.WALSize,
			ActiveMemTableSize: stats.ActiveMemTableSize,
		},
	}

	s.writeJSONResponse(w, response, http.StatusOK)
}

// handleHealthy returns 200 if the server is healthy.
func (s *Server) handleHealthy(w http.ResponseWriter, r *http.Request) {
	response := HealthResponse{
		Status:  "healthy",
		Message: "TSDB is operational",
	}
	s.writeJSONResponse(w, response, http.StatusOK)
}

// handleReady returns 200 if the server is ready to accept requests.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	response := HealthResponse{
		Status:  "ready",
		Message: "TSDB is ready to serve requests",
	}
	s.writeJSONResponse(w, response, http.StatusOK)
}

// writeJSONResponse writes a JSON response.
func (s *Server) writeJSONResponse(w http.ResponseWriter, data interface{}, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Error encoding JSON response: %v", err)
	}
}

// writeErrorResponse writes an error response.
func (s *Server) writeErrorResponse(w http.ResponseWriter, errMsg string, statusCode int) {
	response := QueryResponse{
		Status: "error",
		Error:  errMsg,
	}
	s.writeJSONResponse(w, response, statusCode)
}

// parseMatchers parses a query string into label matchers.
// Example: {__name__="cpu_usage",host="server1"}
// This is a simplified parser for the basic format.
func parseMatchers(queryStr string) (index.Matchers, error) {
	queryStr = strings.TrimSpace(queryStr)

	// Simple parsing: expect format {label="value",label2="value2"}
	if !strings.HasPrefix(queryStr, "{") || !strings.HasSuffix(queryStr, "}") {
		return nil, fmt.Errorf("query must be in format {label=\"value\",...}")
	}

	// Remove braces
	queryStr = strings.TrimPrefix(queryStr, "{")
	queryStr = strings.TrimSuffix(queryStr, "}")

	if queryStr == "" {
		// Empty matcher matches all series
		return index.Matchers{}, nil
	}

	// Split by comma
	parts := strings.Split(queryStr, ",")
	matchers := make(index.Matchers, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)

		// Parse label="value" or label!="value" or label=~"regex" or label!~"regex"
		var matchType index.MatchType
		var labelName, labelValue string

		if strings.Contains(part, "=~") {
			matchType = index.MatchRegexp
			sides := strings.SplitN(part, "=~", 2)
			labelName = strings.TrimSpace(sides[0])
			labelValue = strings.Trim(strings.TrimSpace(sides[1]), "\"")
		} else if strings.Contains(part, "!~") {
			matchType = index.MatchNotRegexp
			sides := strings.SplitN(part, "!~", 2)
			labelName = strings.TrimSpace(sides[0])
			labelValue = strings.Trim(strings.TrimSpace(sides[1]), "\"")
		} else if strings.Contains(part, "!=") {
			matchType = index.MatchNotEqual
			sides := strings.SplitN(part, "!=", 2)
			labelName = strings.TrimSpace(sides[0])
			labelValue = strings.Trim(strings.TrimSpace(sides[1]), "\"")
		} else if strings.Contains(part, "=") {
			matchType = index.MatchEqual
			sides := strings.SplitN(part, "=", 2)
			labelName = strings.TrimSpace(sides[0])
			labelValue = strings.Trim(strings.TrimSpace(sides[1]), "\"")
		} else {
			return nil, fmt.Errorf("invalid matcher format: %s", part)
		}

		matchers = append(matchers, &index.LabelMatcher{
			Name:  labelName,
			Value: labelValue,
			Type:  matchType,
		})
	}

	return matchers, nil
}
