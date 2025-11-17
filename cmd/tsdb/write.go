package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/therealutkarshpriyadarshi/time/pkg/client"
)

var (
	writeAddr string
	writeTime string
)

var writeCmd = &cobra.Command{
	Use:   "write [query] [value]",
	Short: "Write a metric to the TSDB",
	Long: `Write a time-series metric to the TSDB.

The query should be in the format: metric_name{label1="value1",label2="value2"}
The value should be a floating-point number.

Examples:
  tsdb write 'cpu_usage{host="server1"}' 0.85
  tsdb write 'memory_usage{host="server1",region="us-west"}' 1024.5
  tsdb write --addr=http://localhost:8080 'disk_usage{host="server2"}' 2048.0`,
	Args: cobra.ExactArgs(2),
	RunE: runWrite,
}

func init() {
	writeCmd.Flags().StringVar(&writeAddr, "addr", "http://localhost:8080", "TSDB server address")
	writeCmd.Flags().StringVar(&writeTime, "time", "", "Timestamp (default: now)")
}

func runWrite(cmd *cobra.Command, args []string) error {
	query := args[0]
	valueStr := args[1]

	// Parse value
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return fmt.Errorf("invalid value: %w", err)
	}

	// Parse timestamp
	timestamp := time.Now()
	if writeTime != "" {
		ts, err := parseTimestamp(writeTime)
		if err != nil {
			return fmt.Errorf("invalid timestamp: %w", err)
		}
		timestamp = ts
	}

	// Parse query into labels
	labels, err := parseQuery(query)
	if err != nil {
		return fmt.Errorf("invalid query: %w", err)
	}

	// Create client
	c := client.NewClient(writeAddr)

	// Write metric
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	metric := client.Metric{
		Labels:    labels,
		Timestamp: timestamp,
		Value:     value,
	}

	if err := c.Write(ctx, []client.Metric{metric}); err != nil {
		return fmt.Errorf("write failed: %w", err)
	}

	fmt.Printf("Successfully wrote metric: %s = %f at %s\n", query, value, timestamp.Format(time.RFC3339))
	return nil
}

// parseQuery parses a query string into labels
// Format: metric_name{label1="value1",label2="value2"}
func parseQuery(query string) (map[string]string, error) {
	labels := make(map[string]string)

	// Find metric name
	braceIdx := strings.Index(query, "{")
	if braceIdx == -1 {
		// Simple metric name without labels
		labels["__name__"] = query
		return labels, nil
	}

	metricName := strings.TrimSpace(query[:braceIdx])
	if metricName != "" {
		labels["__name__"] = metricName
	}

	// Parse labels
	labelStr := query[braceIdx:]
	if !strings.HasPrefix(labelStr, "{") || !strings.HasSuffix(labelStr, "}") {
		return nil, fmt.Errorf("invalid format: expected {label=\"value\",...}")
	}

	labelStr = strings.TrimPrefix(labelStr, "{")
	labelStr = strings.TrimSuffix(labelStr, "}")

	if labelStr == "" {
		return labels, nil
	}

	// Split by comma
	parts := strings.Split(labelStr, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)

		// Split by =
		eq := strings.Index(part, "=")
		if eq == -1 {
			return nil, fmt.Errorf("invalid label format: %s", part)
		}

		name := strings.TrimSpace(part[:eq])
		value := strings.TrimSpace(part[eq+1:])
		value = strings.Trim(value, "\"")

		labels[name] = value
	}

	return labels, nil
}

// parseTimestamp parses various timestamp formats
func parseTimestamp(s string) (time.Time, error) {
	// Try Unix timestamp (milliseconds)
	if ms, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.UnixMilli(ms), nil
	}

	// Try RFC3339
	if ts, err := time.Parse(time.RFC3339, s); err == nil {
		return ts, nil
	}

	// Try other common formats
	formats := []string{
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, format := range formats {
		if ts, err := time.Parse(format, s); err == nil {
			return ts, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse timestamp: %s", s)
}
