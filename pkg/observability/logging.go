package observability

import (
	"context"
	"log/slog"
	"os"
	"runtime"
	"time"
)

// LogLevel represents logging levels
type LogLevel string

const (
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
)

var (
	defaultLogger *slog.Logger
)

func init() {
	// Initialize with JSON handler by default
	defaultLogger = NewLogger(LogLevelInfo, true)
}

// NewLogger creates a new structured logger
func NewLogger(level LogLevel, jsonFormat bool) *slog.Logger {
	var slogLevel slog.Level

	switch level {
	case LogLevelDebug:
		slogLevel = slog.LevelDebug
	case LogLevelInfo:
		slogLevel = slog.LevelInfo
	case LogLevelWarn:
		slogLevel = slog.LevelWarn
	case LogLevelError:
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: slogLevel,
		AddSource: true,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Shorten source file paths
			if a.Key == slog.SourceKey {
				if source, ok := a.Value.Any().(*slog.Source); ok {
					// Get relative path
					source.File = shortFile(source.File)
				}
			}
			return a
		},
	}

	var handler slog.Handler
	if jsonFormat {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	return slog.New(handler)
}

// SetDefaultLogger sets the global default logger
func SetDefaultLogger(logger *slog.Logger) {
	defaultLogger = logger
	slog.SetDefault(logger)
}

// GetDefaultLogger returns the default logger
func GetDefaultLogger() *slog.Logger {
	return defaultLogger
}

func shortFile(file string) string {
	// Keep only the last 2 path components
	short := file
	for i := len(file) - 1; i > 0; i-- {
		if file[i] == '/' {
			short = file[i+1:]
			for j := i - 1; j > 0; j-- {
				if file[j] == '/' {
					short = file[j+1:]
					break
				}
			}
			break
		}
	}
	return short
}

// LoggerContext adds logger to context
func LoggerContext(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

type loggerKey struct{}

// LoggerFromContext retrieves logger from context
func LoggerFromContext(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok {
		return logger
	}
	return defaultLogger
}

// LoggingMiddleware provides request logging for HTTP handlers
func LoggingMiddleware(logger *slog.Logger) func(next func()) func() {
	return func(next func()) func() {
		return func() {
			start := time.Now()

			// Call next handler
			next()

			duration := time.Since(start)

			logger.Info("request completed",
				"duration_ms", duration.Milliseconds(),
			)
		}
	}
}

// LogStartup logs application startup information
func LogStartup(logger *slog.Logger, version, listenAddr, dataDir string, config map[string]interface{}) {
	logger.Info("starting TSDB server",
		"version", version,
		"listen_addr", listenAddr,
		"data_dir", dataDir,
		"go_version", runtime.Version(),
		"num_cpu", runtime.NumCPU(),
	)

	for k, v := range config {
		logger.Info("configuration", k, v)
	}
}

// LogShutdown logs application shutdown
func LogShutdown(logger *slog.Logger, reason string) {
	logger.Info("shutting down TSDB server", "reason", reason)
}

// LogPanic logs panic information and stack trace
func LogPanic(logger *slog.Logger, recovered interface{}) {
	stackBuf := make([]byte, 4096)
	n := runtime.Stack(stackBuf, false)
	stack := string(stackBuf[:n])

	logger.Error("panic recovered",
		"panic", recovered,
		"stack", stack,
	)
}

// LogMemTableFlush logs MemTable flush events
func LogMemTableFlush(logger *slog.Logger, seriesCount, sampleCount int, duration time.Duration) {
	logger.Info("flushed memtable",
		"series_count", seriesCount,
		"sample_count", sampleCount,
		"duration_ms", duration.Milliseconds(),
	)
}

// LogWALReplay logs WAL replay events
func LogWALReplay(logger *slog.Logger, entriesReplayed int, duration time.Duration) {
	logger.Info("replayed WAL",
		"entries", entriesReplayed,
		"duration_ms", duration.Milliseconds(),
	)
}

// LogCompaction logs compaction events
func LogCompaction(logger *slog.Logger, level int, inputBlocks, outputBlocks int, inputBytes, outputBytes int64, duration time.Duration) {
	compressionRatio := 0.0
	if inputBytes > 0 {
		compressionRatio = float64(inputBytes) / float64(outputBytes)
	}

	logger.Info("compaction completed",
		"level", level,
		"input_blocks", inputBlocks,
		"output_blocks", outputBlocks,
		"input_bytes", inputBytes,
		"output_bytes", outputBytes,
		"compression_ratio", compressionRatio,
		"duration_ms", duration.Milliseconds(),
	)
}

// LogRetention logs retention policy execution
func LogRetention(logger *slog.Logger, blocksDeleted int, bytesFreed int64, duration time.Duration) {
	logger.Info("retention policy applied",
		"blocks_deleted", blocksDeleted,
		"bytes_freed", bytesFreed,
		"duration_ms", duration.Milliseconds(),
	)
}

// LogQuery logs query execution
func LogQuery(logger *slog.Logger, query string, seriesMatched, samplesReturned int, duration time.Duration) {
	logger.Debug("query executed",
		"query", query,
		"series_matched", seriesMatched,
		"samples_returned", samplesReturned,
		"duration_ms", duration.Milliseconds(),
	)
}

// LogError logs an error with context
func LogError(logger *slog.Logger, operation string, err error, attrs ...any) {
	args := []any{"operation", operation, "error", err}
	args = append(args, attrs...)
	logger.Error("operation failed", args...)
}

// LogInsert logs sample insertion
func LogInsert(logger *slog.Logger, seriesHash uint64, sampleCount int, duration time.Duration) {
	logger.Debug("samples inserted",
		"series_hash", seriesHash,
		"sample_count", sampleCount,
		"duration_us", duration.Microseconds(),
	)
}

// LogBlockCreated logs block creation
func LogBlockCreated(logger *slog.Logger, blockID string, minTime, maxTime int64, seriesCount, chunkCount int) {
	logger.Info("block created",
		"block_id", blockID,
		"min_time", minTime,
		"max_time", maxTime,
		"series_count", seriesCount,
		"chunk_count", chunkCount,
	)
}

// LogIndexBuild logs index building
func LogIndexBuild(logger *slog.Logger, seriesCount int, duration time.Duration) {
	logger.Info("index built",
		"series_count", seriesCount,
		"duration_ms", duration.Milliseconds(),
	)
}
