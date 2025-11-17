package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/therealutkarshpriyadarshi/time/pkg/client"
)

func main() {
	// Create a new client
	c := client.NewClient("http://localhost:8080")

	ctx := context.Background()

	// Write some metrics
	fmt.Println("Writing metrics...")
	metrics := []client.Metric{
		{
			Labels: map[string]string{
				"__name__": "cpu_usage",
				"host":     "server1",
				"region":   "us-west",
			},
			Timestamp: time.Now(),
			Value:     0.75,
		},
		{
			Labels: map[string]string{
				"__name__": "memory_usage",
				"host":     "server1",
				"region":   "us-west",
			},
			Timestamp: time.Now(),
			Value:     1024.5,
		},
		{
			Labels: map[string]string{
				"__name__": "disk_usage",
				"host":     "server1",
				"region":   "us-west",
			},
			Timestamp: time.Now(),
			Value:     2048.0,
		},
	}

	if err := c.Write(ctx, metrics); err != nil {
		log.Fatalf("Failed to write metrics: %v", err)
	}

	fmt.Println("Metrics written successfully!")

	// Wait a bit for data to be available
	time.Sleep(500 * time.Millisecond)

	// Query the data back
	fmt.Println("\nQuerying metrics...")
	results, err := c.Query(ctx, `{__name__="cpu_usage",host="server1"}`, time.Now())
	if err != nil {
		log.Fatalf("Failed to query: %v", err)
	}

	fmt.Printf("Found %d series\n", len(results))
	for _, result := range results {
		fmt.Printf("  Labels: %v\n", result.Labels)
		if len(result.Samples) > 0 {
			fmt.Printf("  Latest value: %f at %s\n", result.Samples[0].Value, result.Samples[0].Timestamp.Format(time.RFC3339))
		}
	}

	// Range query
	fmt.Println("\nRange query...")
	start := time.Now().Add(-1 * time.Hour)
	end := time.Now()
	step := 1 * time.Minute

	rangeResults, err := c.QueryRange(ctx, `{host="server1"}`, start, end, step)
	if err != nil {
		log.Fatalf("Failed to query range: %v", err)
	}

	fmt.Printf("Found %d series in range\n", len(rangeResults))
	for i, result := range rangeResults {
		fmt.Printf("\nSeries %d:\n", i+1)
		fmt.Printf("  Labels: %v\n", result.Labels)
		fmt.Printf("  Samples: %d\n", len(result.Samples))
		if len(result.Samples) > 0 {
			fmt.Printf("  First: %f at %s\n", result.Samples[0].Value, result.Samples[0].Timestamp.Format(time.RFC3339))
			if len(result.Samples) > 1 {
				lastIdx := len(result.Samples) - 1
				fmt.Printf("  Last:  %f at %s\n", result.Samples[lastIdx].Value, result.Samples[lastIdx].Timestamp.Format(time.RFC3339))
			}
		}
	}

	// Get labels
	fmt.Println("\nFetching labels...")
	labels, err := c.Labels(ctx)
	if err != nil {
		log.Fatalf("Failed to get labels: %v", err)
	}

	fmt.Printf("Labels: %v\n", labels)

	// Get label values
	fmt.Println("\nFetching values for 'host' label...")
	values, err := c.LabelValues(ctx, "host")
	if err != nil {
		log.Fatalf("Failed to get label values: %v", err)
	}

	fmt.Printf("Host values: %v\n", values)

	// Check health
	fmt.Println("\nChecking health...")
	healthy, err := c.Health(ctx)
	if err != nil {
		log.Fatalf("Failed to check health: %v", err)
	}

	if healthy {
		fmt.Println("✓ TSDB is healthy")
	} else {
		fmt.Println("✗ TSDB is unhealthy")
	}
}
