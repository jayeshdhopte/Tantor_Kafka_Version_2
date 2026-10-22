package logger

import (
	"io"
	"log/slog"
	"os"
)

// InitLogger initializes the global structured logger.
func InitLogger(level string) {
	InitLoggerOutput(level, os.Stdout)
}

func InitLoggerOutput(level string, output io.Writer) {
	var slogLevel slog.Level
	switch level {
	case "DEBUG", "debug":
		slogLevel = slog.LevelDebug
	case "INFO", "info":
		slogLevel = slog.LevelInfo
	case "WARN", "warn":
		slogLevel = slog.LevelWarn
	case "ERROR", "error":
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: slogLevel,
	}

	handler := slog.NewJSONHandler(output, opts)
	logger := slog.New(handler)
	slog.SetDefault(logger)
}
