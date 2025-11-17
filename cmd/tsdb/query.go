package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/therealutkarshpriyadarshi/time/pkg/client"
)

var (
	queryAddr  string
	queryStart string
	queryEnd   string
	queryStep  string
)

var queryCmd = &cobra.Command{
	Use:   "query [query]",
	Short: "Query metrics from the TSDB",
	Long: `Query time-series metrics from the TSDB.

For instant queries (default), returns the latest value.
For range queries (with --start and --end), returns all values in the range.

Examples:
  # Instant query
  tsdb query 'cpu_usage{host="server1"}'

  # Range query
  tsdb query 'cpu_usage{host="server1"}' --start=-1h --end=now --step=1m

  # Range query with explicit timestamps
  tsdb query 'memory_usage{host="server1"}' --start=2024-01-01T00:00:00 --end=2024-01-01T01:00:00`,
	Args: cobra.ExactArgs(1),
	RunE: runQuery,
}

func init() {
	queryCmd.Flags().StringVar(&queryAddr, "addr", "http://localhost:8080", "TSDB server address")
	queryCmd.Flags().StringVar(&queryStart, "start", "", "Start time (for range queries)")
	queryCmd.Flags().StringVar(&queryEnd, "end", "", "End time (for range queries)")
	queryCmd.Flags().StringVar(&queryStep, "step", "1m", "Query step (for range queries)")
}

func runQuery(cmd *cobra.Command, args []string) error {
	query := args[0]

	// Create client
	c := client.NewClient(queryAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Check if this is a range query
	if queryStart != "" || queryEnd != "" {
		return runRangeQuery(ctx, c, query)
	}

	return runInstantQuery(ctx, c, query)
}

func runInstantQuery(ctx context.Context, c *client.Client, query string) error {
	// Execute instant query
	results, err := c.Query(ctx, query, time.Now())
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}

	if len(results) == 0 {
		fmt.Println("No results found")
		return nil
	}

	// Print results
	fmt.Printf("Results (%d series):\n\n", len(results))
	for i, result := range results {
		fmt.Printf("Series %d:\n", i+1)
		fmt.Printf("  Labels: %s\n", formatLabels(result.Labels))

		if len(result.Samples) > 0 {
			sample := result.Samples[0]
			fmt.Printf("  Value: %f at %s\n", sample.Value, sample.Timestamp.Format(time.RFC3339))
		}
		fmt.Println()
	}

	return nil
}

func runRangeQuery(ctx context.Context, c *client.Client, query string) error {
	// Parse start time
	var start time.Time
	var err error
	if queryStart == "" {
		start = time.Now().Add(-1 * time.Hour) // Default: 1 hour ago
	} else {
		start, err = parseTimeOrRelative(queryStart)
		if err != nil {
			return fmt.Errorf("invalid start time: %w", err)
		}
	}

	// Parse end time
	var end time.Time
	if queryEnd == "" || queryEnd == "now" {
		end = time.Now()
	} else {
		end, err = parseTimeOrRelative(queryEnd)
		if err != nil {
			return fmt.Errorf("invalid end time: %w", err)
		}
	}

	// Parse step
	step, err := time.ParseDuration(queryStep)
	if err != nil {
		return fmt.Errorf("invalid step: %w", err)
	}

	// Execute range query
	results, err := c.QueryRange(ctx, query, start, end, step)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}

	if len(results) == 0 {
		fmt.Println("No results found")
		return nil
	}

	// Print results
	fmt.Printf("Results (%d series):\n", len(results))
	fmt.Printf("Time range: %s to %s (step: %s)\n\n", start.Format(time.RFC3339), end.Format(time.RFC3339), step)

	for i, result := range results {
		fmt.Printf("Series %d:\n", i+1)
		fmt.Printf("  Labels: %s\n", formatLabels(result.Labels))
		fmt.Printf("  Samples (%d):\n", len(result.Samples))

		// Print up to 10 samples
		maxSamples := 10
		if len(result.Samples) < maxSamples {
			maxSamples = len(result.Samples)
		}

		for j := 0; j < maxSamples; j++ {
			sample := result.Samples[j]
			fmt.Printf("    %s: %f\n", sample.Timestamp.Format(time.RFC3339), sample.Value)
		}

		if len(result.Samples) > maxSamples {
			fmt.Printf("    ... and %d more samples\n", len(result.Samples)-maxSamples)
		}

		fmt.Println()
	}

	return nil
}

// formatLabels formats labels for display
func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return "{}"
	}

	var parts []string
	for name, value := range labels {
		parts = append(parts, fmt.Sprintf("%s=\"%s\"", name, value))
	}

	return "{" + strings.Join(parts, ", ") + "}"
}

// parseTimeOrRelative parses a time string that can be absolute or relative
func parseTimeOrRelative(s string) (time.Time, error) {
	// Handle relative times (e.g., -1h, -30m)
	if strings.HasPrefix(s, "-") {
		duration, err := time.ParseDuration(s[1:])
		if err != nil {
			return time.Time{}, err
		}
		return time.Now().Add(-duration), nil
	}

	// Handle "now"
	if s == "now" {
		return time.Now(), nil
	}

	// Try to parse as timestamp
	return parseTimestamp(s)
}
