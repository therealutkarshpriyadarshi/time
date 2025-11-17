package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/therealutkarshpriyadarshi/time/pkg/client"
)

// This example demonstrates how to use the TSDB for monitoring system metrics

func main() {
	// Create client
	c := client.NewClient("http://localhost:8080")

	// Simulate monitoring 3 servers
	servers := []string{"server1", "server2", "server3"}

	fmt.Println("Starting monitoring simulation...")
	fmt.Println("Writing metrics every 10 seconds for 1 minute")
	fmt.Println("Press Ctrl+C to stop\n")

	// Run for 1 minute, writing every 10 seconds
	duration := 1 * time.Minute
	interval := 10 * time.Second
	endTime := time.Now().Add(duration)

	for time.Now().Before(endTime) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		// Collect metrics for all servers
		metrics := make([]client.Metric, 0)
		now := time.Now()

		for _, server := range servers {
			// Simulate CPU usage (0-100%)
			cpuUsage := rand.Float64() * 100

			// Simulate memory usage (0-16GB)
			memoryUsage := rand.Float64() * 16 * 1024

			// Simulate disk usage (0-1TB)
			diskUsage := rand.Float64() * 1024 * 1024

			// Simulate request count (0-10000 requests/sec)
			requestCount := rand.Float64() * 10000

			metrics = append(metrics,
				client.Metric{
					Labels: map[string]string{
						"__name__": "cpu_usage_percent",
						"host":     server,
						"region":   "us-west",
					},
					Timestamp: now,
					Value:     cpuUsage,
				},
				client.Metric{
					Labels: map[string]string{
						"__name__": "memory_usage_mb",
						"host":     server,
						"region":   "us-west",
					},
					Timestamp: now,
					Value:     memoryUsage,
				},
				client.Metric{
					Labels: map[string]string{
						"__name__": "disk_usage_mb",
						"host":     server,
						"region":   "us-west",
					},
					Timestamp: now,
					Value:     diskUsage,
				},
				client.Metric{
					Labels: map[string]string{
						"__name__": "http_requests_total",
						"host":     server,
						"region":   "us-west",
					},
					Timestamp: now,
					Value:     requestCount,
				},
			)
		}

		// Write all metrics
		if err := c.Write(ctx, metrics); err != nil {
			log.Printf("Error writing metrics: %v", err)
		} else {
			fmt.Printf("[%s] Wrote %d metrics\n", now.Format("15:04:05"), len(metrics))
		}

		cancel()

		// Wait for next interval
		time.Sleep(interval)
	}

	fmt.Println("\nMonitoring simulation complete!")
	fmt.Println("\nQuerying data...")

	// Query the collected data
	ctx := context.Background()

	// Get CPU usage for all servers
	start := time.Now().Add(-duration - 1*time.Minute)
	end := time.Now()
	step := 10 * time.Second

	results, err := c.QueryRange(ctx, `{__name__="cpu_usage_percent"}`, start, end, step)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	fmt.Printf("\nCPU Usage Summary (%d series):\n", len(results))
	fmt.Println("=" + string(make([]byte, 60)))

	for _, result := range results {
		host := result.Labels["host"]
		if len(result.Samples) == 0 {
			continue
		}

		// Calculate average, min, max
		var sum, min, max float64
		min = result.Samples[0].Value
		max = result.Samples[0].Value

		for _, sample := range result.Samples {
			sum += sample.Value
			if sample.Value < min {
				min = sample.Value
			}
			if sample.Value > max {
				max = sample.Value
			}
		}

		avg := sum / float64(len(result.Samples))

		fmt.Printf("\n%s:\n", host)
		fmt.Printf("  Samples:  %d\n", len(result.Samples))
		fmt.Printf("  Average:  %.2f%%\n", avg)
		fmt.Printf("  Min:      %.2f%%\n", min)
		fmt.Printf("  Max:      %.2f%%\n", max)
	}

	// Get all available labels
	labels, err := c.Labels(ctx)
	if err != nil {
		log.Fatalf("Failed to get labels: %v", err)
	}

	fmt.Printf("\nAvailable metrics: %v\n", labels)
}
