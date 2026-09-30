package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type ExternalClusterPayload struct {
	HostID              string  `json:"hostId"`
	AgentName           string  `json:"agentName"`
	Name                string  `json:"name"`
	Environment         string  `json:"environment"`
	BootstrapServers    string  `json:"bootstrapServers"`
	KafkaVersion        string  `json:"kafkaVersion"`
	KafkaClusterID      string  `json:"kafkaClusterId"`
	KafkaMode           string  `json:"kafkaMode"`
	Security            string  `json:"security"`
	BrokerCount         int     `json:"brokerCount"`
	LocalBrokerCount    int     `json:"localBrokerCount"`
	NodeID              int     `json:"nodeId"`
	IsRunning           bool    `json:"isRunning"`
	InstallPath         string  `json:"installPath"`
	ConfigFile          string  `json:"configFile"`
	DataDirs            string  `json:"dataDirs"`
	LogDirs             string  `json:"logDirs"`
	Hostname            string  `json:"hostname"`
	Listeners           string  `json:"listeners"`
	AdvertisedListeners string  `json:"advertisedListeners"`
	ProcessRoles        string  `json:"processRoles"`
	CPUUsagePct         float64 `json:"cpuUsagePct"`
	MemoryUsedMB        int64   `json:"memoryUsedMb"`
	MemoryTotalMB       int64   `json:"memoryTotalMb"`
	DiskUsedGB          int64   `json:"diskUsedGb"`
	DiskTotalGB         int64   `json:"diskTotalGb"`
	DiskUsedBytes       int64   `json:"diskUsedBytes"`
	DiskTotalBytes      int64   `json:"diskTotalBytes"`
	JMXExporterPort     *int    `json:"jmxExporterPort,omitempty"`
	CanExecuteTasks     bool    `json:"canExecuteTasks"`
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func externalAgentPath(clusterName, suffix string) string {
	return "/api/v1/ui/external-clusters/discovery/" + url.PathEscape(clusterName) + suffix
}

func configuredJMXExporterPort(metricsURL string) *int {
	rawURL := strings.ReplaceAll(strings.TrimSpace(metricsURL), "{host}", "127.0.0.1")
	if rawURL == "" {
		return nil
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Port() == "" {
		return nil
	}
	port, err := strconv.Atoi(parsed.Port())
	if err != nil || port < 1 || port > 65535 {
		return nil
	}
	return &port
}

func jmxMetricsURLForCluster(cfg RuntimeConfig, cluster DiscoveredCluster) string {
	controllerOnly := roleContains(cluster.ProcessRoles, "controller") && !roleContains(cluster.ProcessRoles, "broker")
	if controllerOnly {
		return cfg.ControllerJMXMetricsURL
	}
	return cfg.JMXMetricsURL
}

func registerCluster(ctx context.Context, client *APIClient, cfg RuntimeConfig, c DiscoveredCluster, hostMetrics HostMetrics, logger *slog.Logger) bool {
	payload := ExternalClusterPayload{
		HostID:              cfg.HostID,
		AgentName:           cfg.AgentName,
		Name:                c.Name,
		Environment:         c.Environment,
		BootstrapServers:    c.BootstrapServers,
		KafkaVersion:        c.KafkaVersion,
		KafkaClusterID:      c.KafkaClusterID,
		KafkaMode:           c.KafkaMode,
		Security:            c.Security,
		BrokerCount:         c.BrokerCount,
		LocalBrokerCount:    c.BrokerCount,
		NodeID:              c.NodeID,
		IsRunning:           c.IsRunning,
		InstallPath:         c.InstallPath,
		ConfigFile:          c.PropsFile,
		DataDirs:            firstNonBlank(c.DataDirs, c.LogDirs),
		LogDirs:             c.LogDirs,
		Hostname:            c.Hostname,
		Listeners:           c.Listeners,
		AdvertisedListeners: c.AdvertisedListeners,
		ProcessRoles:        c.ProcessRoles,
		CPUUsagePct:         hostMetrics.CPUUsagePct,
		MemoryUsedMB:        hostMetrics.MemoryUsedMB,
		MemoryTotalMB:       hostMetrics.MemoryTotalMB,
		DiskUsedGB:          hostMetrics.DiskUsedGB,
		DiskTotalGB:         hostMetrics.DiskTotalGB,
		DiskUsedBytes:       hostMetrics.DiskUsedBytes,
		DiskTotalBytes:      hostMetrics.DiskTotalBytes,
		JMXExporterPort:     configuredJMXExporterPort(jmxMetricsURLForCluster(cfg, c)),
		CanExecuteTasks:     cfg.EnableTasks,
	}

	_, err := client.DoJSON(ctx, http.MethodPost, client.endpoint("/api/v1/ui/external-clusters/discovery/report"), nil, payload, nil)
	if err != nil {
		logger.Error("cluster registration failed", "cluster", c.Name, "bootstrap", c.BootstrapServers, "error", err)
		return false
	}
	logger.Info("cluster registered", "cluster", c.Name, "bootstrap", c.BootstrapServers, "node_id", c.NodeID)
	return true
}

func completeTaskWithResult(ctx context.Context, client *APIClient, cluster DiscoveredCluster, hostname string, result AgentTaskResult) error {
	query := url.Values{}
	query.Set("hostname", hostname)
	query.Set("bootstrap", cluster.BootstrapServers)
	_, err := client.DoJSON(ctx, http.MethodPost, client.endpoint(externalAgentPath(cluster.Name, "/tasks/complete")), query, result, nil)
	if err != nil {
		return fmt.Errorf("complete task %s: %w", result.TaskID, err)
	}
	return nil
}

func reportOfflineCluster(ctx context.Context, client *APIClient, cfg RuntimeConfig, c DiscoveredCluster, logger *slog.Logger) {
	c.IsRunning = false
	metrics := collectClusterMetrics(ctx, c)
	if !registerCluster(ctx, client, cfg, c, metrics, logger) {
		logger.Warn("failed to report Kafka node offline", "cluster", c.Name, "node_id", c.NodeID, "config", c.PropsFile)
		return
	}
	logger.Info("Kafka node reported offline", "cluster", c.Name, "node_id", c.NodeID, "config", c.PropsFile)
}
