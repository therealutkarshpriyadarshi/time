package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	version = "1.0.0"
	commit  = "dev"
	date    = "unknown"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "tsdb",
	Short: "TSDB - A production-grade time-series database",
	Long: `TSDB is a high-performance time-series database optimized for
monitoring metrics and observability data.

Features:
  - High write throughput (100K-500K samples/sec)
  - Fast queries (<100ms for 1-week range)
  - Efficient compression (10-20x ratio)
  - Production-ready with WAL and crash recovery`,
	Version: fmt.Sprintf("%s (commit: %s, built: %s)", version, commit, date),
}

func init() {
	rootCmd.AddCommand(startCmd)
	rootCmd.AddCommand(writeCmd)
	rootCmd.AddCommand(queryCmd)
	rootCmd.AddCommand(inspectCmd)
}
