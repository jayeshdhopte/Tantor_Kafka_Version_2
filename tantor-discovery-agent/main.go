package main

import (
	"context"
	"fmt"
	"io"
	"io.translab/tantor-agent/pkg/logrotation"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var buildVersion = "3.4.0-dataservices.2-configchange.9-rollingrestart.4-logging.1"
var buildCommit = "unknown"

func newLogger(level string) *slog.Logger {
	return newLoggerOutput(level, os.Stdout)
}
func newLoggerOutput(level string, output io.Writer) *slog.Logger {
	var slogLevel slog.Level
	switch strings.ToLower(level) {
	case "debug":
		slogLevel = slog.LevelDebug
	case "warn", "warning":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{Level: slogLevel}))
}

func main() {
	if handled, err := logrotation.PruneCommand(os.Args[1:], env("TANTOR_AGENT_LOG_DIR", "/var/log/tantor/discovery-agent"), "tantor-discovery-agent.log"); handled {
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	for _, arg := range os.Args[1:] {
		if arg == "--version" || arg == "-version" {
			fmt.Printf("tantor-discovery-agent %s commit=%s\n", buildVersion, buildCommit)
			return
		}
	}
	cfg, err := LoadRuntimeConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "configuration error:", err)
		os.Exit(2)
	}

	if cfg.PrecheckJSON {
		runPrecheck(true, cfg.ScanPaths, cfg.DiscoveryPolicy)
		return
	}
	if cfg.PrecheckOnly {
		runPrecheck(false, cfg.ScanPaths, cfg.DiscoveryPolicy)
		return
	}

	logFile, err := logrotation.New(cfg.LogDir, "tantor-discovery-agent.log")
	if err != nil {
		fmt.Fprintln(os.Stderr, "agent log initialization:", err)
		os.Exit(2)
	}
	defer logFile.Close()
	logger := newLoggerOutput(cfg.LogLevel, logFile)
	if cfg.EnableServiceDeployment && os.Geteuid() != 0 {
		logger.Error("service deployment requires a root-run Discovery agent; monitoring-only use remains supported")
		os.Exit(2)
	}
	if cfg.RunPrecheck {
		runPrecheck(false, cfg.ScanPaths, cfg.DiscoveryPolicy)
	}
	if parsed, _ := url.Parse(cfg.ServerURL); parsed != nil && parsed.Scheme == "http" {
		logger.Warn("backend connection is using plaintext HTTP; use HTTPS in production", "server", cfg.ServerURL)
	}
	if cfg.InsecureSkipVerify {
		logger.Warn("TLS certificate verification is disabled by explicit configuration")
	}

	client, err := NewAPIClient(cfg, logger)
	if err != nil {
		logger.Error("failed to initialize backend client", "error", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logFile.Start(ctx)
	if err := logrotation.InstallRetention(ctx, cfg.LogDir, "tantor-discovery-agent.log", "tantor-discovery-agent"); err != nil {
		logger.Warn("Log retention timer installation failed; in-process rotation remains active", "error", err)
	}
	store := &ClusterStore{}

	logger.Info("Tantor discovery agent starting",
		"server", cfg.ServerURL,
		"host_id", cfg.HostID,
		"agent_name", cfg.AgentName,
		"node_name", cfg.NodeName,
		"environment", cfg.Environment,
		"scan_paths", cfg.ScanPaths,
		"discovery_policy", cfg.DiscoveryPolicy,
		"run_user_uid", os.Geteuid(),
		"version", buildVersion,
	)

	clusters := runDiscovery(ctx, client, cfg, logger)
	store.Set(clusters)
	if cfg.Once {
		return
	}

	if cfg.EnableTasks {
		go runTaskPoller(ctx, client, cfg, store, logger)
		go runManagedPoller(ctx, client, cfg, store, logger)
	}
	go runMetricsLoop(ctx, client, cfg, store, logger)
	if cfg.EnableServiceDeployment {
		go runServicePoller(ctx, client, cfg, store, logger)
	}

	ticker := time.NewTicker(cfg.DiscoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Info("agent shutting down", "reason", ctx.Err())
			return
		case <-ticker.C:
			previous := store.Get()
			current := runDiscovery(ctx, client, cfg, logger)
			currentIDs := map[string]bool{}
			for _, c := range current {
				currentIDs[nodeIdentity(c)] = true
			}
			for _, old := range previous {
				if old.IsRunning && !currentIDs[nodeIdentity(old)] {
					reportOfflineCluster(ctx, client, cfg, old, logger)
				}
			}
			store.Set(current)
		}
	}
}
