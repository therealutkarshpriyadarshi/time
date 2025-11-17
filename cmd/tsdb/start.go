package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/therealutkarshpriyadarshi/time/pkg/api"
	"github.com/therealutkarshpriyadarshi/time/pkg/storage"
)

var (
	listenAddr         string
	dataDir            string
	retention          string
	enableCompaction   bool
	enableRetention    bool
	flushInterval      string
	compactionInterval string
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the TSDB server",
	Long: `Start the TSDB server with HTTP API.

The server will listen on the specified address and serve the HTTP API
for writing and querying time-series data.

Example:
  tsdb start --listen=:8080 --data-dir=./data --retention=30d`,
	RunE: runStart,
}

func init() {
	startCmd.Flags().StringVar(&listenAddr, "listen", ":8080", "HTTP listen address")
	startCmd.Flags().StringVar(&dataDir, "data-dir", "./data", "Data directory path")
	startCmd.Flags().StringVar(&retention, "retention", "30d", "Data retention period (e.g., 30d, 7d, 24h)")
	startCmd.Flags().BoolVar(&enableCompaction, "enable-compaction", true, "Enable background compaction")
	startCmd.Flags().BoolVar(&enableRetention, "enable-retention", true, "Enable retention policy")
	startCmd.Flags().StringVar(&flushInterval, "flush-interval", "30s", "MemTable flush interval")
	startCmd.Flags().StringVar(&compactionInterval, "compaction-interval", "10m", "Compaction check interval")
}

func runStart(cmd *cobra.Command, args []string) error {
	log.Printf("Starting TSDB server...")
	log.Printf("  Listen address: %s", listenAddr)
	log.Printf("  Data directory: %s", dataDir)
	log.Printf("  Retention: %s", retention)
	log.Printf("  Compaction: %v", enableCompaction)

	// Parse durations
	retentionDuration, err := parseDuration(retention)
	if err != nil {
		return fmt.Errorf("invalid retention: %w", err)
	}

	flushIntervalDuration, err := time.ParseDuration(flushInterval)
	if err != nil {
		return fmt.Errorf("invalid flush interval: %w", err)
	}

	compactionIntervalDuration, err := time.ParseDuration(compactionInterval)
	if err != nil {
		return fmt.Errorf("invalid compaction interval: %w", err)
	}

	// Create TSDB options
	opts := storage.DefaultOptions(dataDir)
	opts.RetentionPeriod = retentionDuration
	opts.EnableCompaction = enableCompaction
	opts.EnableRetention = enableRetention
	opts.FlushInterval = flushIntervalDuration
	opts.CompactionInterval = compactionIntervalDuration

	// Open TSDB
	log.Printf("Opening TSDB at %s...", dataDir)
	db, err := storage.Open(opts)
	if err != nil {
		return fmt.Errorf("failed to open TSDB: %w", err)
	}
	defer db.Close()

	log.Printf("TSDB opened successfully")

	// Create API server
	server := api.NewServer(db, listenAddr)

	// Start server in a goroutine
	serverErr := make(chan error, 1)
	go func() {
		log.Printf("Starting HTTP API server on %s", listenAddr)
		if err := server.Start(); err != nil {
			serverErr <- err
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		return fmt.Errorf("server error: %w", err)
	case sig := <-sigChan:
		log.Printf("Received signal %s, shutting down...", sig)
	}

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log.Printf("Shutting down HTTP server...")
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	log.Printf("Closing TSDB...")
	if err := db.Close(); err != nil {
		log.Printf("TSDB close error: %v", err)
	}

	log.Printf("Shutdown complete")
	return nil
}

// parseDuration parses a duration string with support for days
func parseDuration(s string) (time.Duration, error) {
	// Check for days suffix
	if len(s) > 1 && s[len(s)-1] == 'd' {
		days := s[:len(s)-1]
		var d int
		_, err := fmt.Sscanf(days, "%d", &d)
		if err != nil {
			return 0, err
		}
		return time.Duration(d) * 24 * time.Hour, nil
	}

	return time.ParseDuration(s)
}
