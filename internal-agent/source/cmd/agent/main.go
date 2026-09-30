package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"io.translab/tantor-agent/internal/client"
	"io.translab/tantor-agent/internal/collect"
	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/internal/deploy"
	"io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/internal/task"
	"io.translab/tantor-agent/pkg/logger"
)

var (
	version   = "1.0.0-prod.16-reporting.9-dataservices.1"
	commit    = "dataservices-prechecks"
	buildDate = "2026-09-29"
)

func main() {
	configPath := flag.String("config", "/etc/tantor-agent/agent.yaml", "Path to agent configuration file")
	showVersion := flag.Bool("version", false, "Print version and exit")
	checkConfig := flag.Bool("check-config", false, "Validate configuration and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("tantor-agent version=%s commit=%s build_date=%s\n", version, commit, buildDate)
		return
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(2)
	}
	if *checkConfig {
		fmt.Printf("configuration valid: host_id=%s agent_name=%s server_url=%s privilege_mode=%s\n",
			cfg.Agent.HostID, cfg.Agent.AgentName, cfg.Agent.ServerURL, cfg.Privilege.Mode)
		return
	}

	logger.InitLogger(cfg.Agent.LogLevel)
	slog.Info("Starting Tantor Agent",
		"version", version,
		"commit", commit,
		"hostId", cfg.Agent.HostID,
		"agentName", cfg.Agent.AgentName,
		"serverUrl", cfg.Agent.ServerURL,
		"authMode", cfg.Auth.Mode,
		"privilegeMode", cfg.Privilege.Mode,
	)

	apiClient, err := client.NewAPIClient(cfg, version)
	if err != nil {
		slog.Error("Failed to initialize API client", "err", err)
		os.Exit(1)
	}

	collector := collect.NewCollector(cfg.Agent.HostID, cfg.Agent.AgentName, version, cfg.Agent.ServerURL)
	commandExecutor := executor.New(executor.Options{
		PrivilegeMode: cfg.Privilege.Mode,
		SudoPath:      cfg.Privilege.SudoPath,
	})
	deployEngine := deploy.NewEngine(cfg, apiClient, commandExecutor)
	taskEngine := task.NewEngine(cfg, apiClient, collector, deployEngine)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	taskEngine.Start(ctx)
	slog.Info("Tantor Agent stopped cleanly")
}
