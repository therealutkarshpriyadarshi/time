package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/spf13/cobra"
	"github.com/therealutkarshpriyadarshi/time/pkg/api"
)

var (
	inspectAddr string
)

var inspectCmd = &cobra.Command{
	Use:   "inspect",
	Short: "Inspect TSDB status and metadata",
	Long: `Inspect the TSDB to view its status, statistics, labels, and series.

Examples:
  # View TSDB status
  tsdb inspect status

  # List all labels
  tsdb inspect labels

  # List values for a specific label
  tsdb inspect label-values host

  # View server health
  tsdb inspect health`,
}

var inspectStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Display TSDB status and statistics",
	RunE:  runInspectStatus,
}

var inspectLabelsCmd = &cobra.Command{
	Use:   "labels",
	Short: "List all label names",
	RunE:  runInspectLabels,
}

var inspectLabelValuesCmd = &cobra.Command{
	Use:   "label-values [label-name]",
	Short: "List all values for a specific label",
	Args:  cobra.ExactArgs(1),
	RunE:  runInspectLabelValues,
}

var inspectHealthCmd = &cobra.Command{
	Use:   "health",
	Short: "Check TSDB health",
	RunE:  runInspectHealth,
}

func init() {
	inspectCmd.PersistentFlags().StringVar(&inspectAddr, "addr", "http://localhost:8080", "TSDB server address")

	inspectCmd.AddCommand(inspectStatusCmd)
	inspectCmd.AddCommand(inspectLabelsCmd)
	inspectCmd.AddCommand(inspectLabelValuesCmd)
	inspectCmd.AddCommand(inspectHealthCmd)
}

func runInspectStatus(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	url := inspectAddr + "/api/v1/status/tsdb"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var statusResp api.StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&statusResp); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if statusResp.Status != "success" {
		return fmt.Errorf("request failed: %s", statusResp.Error)
	}

	// Print status
	fmt.Println("TSDB Status:")
	fmt.Println("=============")
	fmt.Printf("Total Samples:       %d\n", statusResp.Data.TotalSamples)
	fmt.Printf("Total Series:        %d\n", statusResp.Data.TotalSeries)
	fmt.Printf("Flush Count:         %d\n", statusResp.Data.FlushCount)
	fmt.Printf("WAL Size:            %d bytes (%.2f MB)\n", statusResp.Data.WALSize, float64(statusResp.Data.WALSize)/(1024*1024))
	fmt.Printf("Active MemTable:     %d bytes (%.2f MB)\n", statusResp.Data.ActiveMemTableSize, float64(statusResp.Data.ActiveMemTableSize)/(1024*1024))

	if statusResp.Data.LastFlushTime > 0 {
		lastFlush := time.UnixMilli(statusResp.Data.LastFlushTime)
		fmt.Printf("Last Flush:          %s (%s ago)\n", lastFlush.Format(time.RFC3339), time.Since(lastFlush).Round(time.Second))
	} else {
		fmt.Printf("Last Flush:          Never\n")
	}

	return nil
}

func runInspectLabels(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	url := inspectAddr + "/api/v1/labels"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var labelsResp api.LabelsResponse
	if err := json.NewDecoder(resp.Body).Decode(&labelsResp); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if labelsResp.Status != "success" {
		return fmt.Errorf("request failed: %s", labelsResp.Error)
	}

	// Print labels
	fmt.Printf("Labels (%d):\n", len(labelsResp.Data))
	fmt.Println("=============")
	for _, label := range labelsResp.Data {
		fmt.Printf("  %s\n", label)
	}

	return nil
}

func runInspectLabelValues(cmd *cobra.Command, args []string) error {
	labelName := args[0]

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	url := fmt.Sprintf("%s/api/v1/label/%s/values", inspectAddr, labelName)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var valuesResp api.LabelValuesResponse
	if err := json.NewDecoder(resp.Body).Decode(&valuesResp); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if valuesResp.Status != "success" {
		return fmt.Errorf("request failed: %s", valuesResp.Error)
	}

	// Print values
	fmt.Printf("Values for label '%s' (%d):\n", labelName, len(valuesResp.Data))
	fmt.Println("=============")
	for _, value := range valuesResp.Data {
		fmt.Printf("  %s\n", value)
	}

	return nil
}

func runInspectHealth(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	url := inspectAddr + "/-/healthy"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Println("✓ TSDB is healthy")
		return nil
	}

	bodyBytes, _ := io.ReadAll(resp.Body)
	fmt.Printf("✗ TSDB is unhealthy (status: %d, body: %s)\n", resp.StatusCode, string(bodyBytes))
	return fmt.Errorf("health check failed")
}
