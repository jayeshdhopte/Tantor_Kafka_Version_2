package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type DiscoveredCluster struct {
	Name                string
	Hostname            string
	BootstrapServers    string
	KafkaVersion        string
	KafkaClusterID      string
	KafkaMode           string
	Security            string
	BrokerCount         int
	NodeID              int
	ProcessRoles        string
	Listeners           string
	AdvertisedListeners string
	IsRunning           bool
	InstallPath         string
	PropsFile           string
	DataDirs            string
	LogDirs             string
	Environment         string
	SystemdService      string
}

func canonicalPathOrClean(path string) string {
	if path == "" {
		return ""
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		if abs, err := filepath.Abs(resolved); err == nil {
			return filepath.Clean(abs)
		}
	}
	if abs, err := filepath.Abs(path); err == nil {
		return filepath.Clean(abs)
	}
	return filepath.Clean(path)
}

func processInfoByConfig(processes []ProcessInfo) map[string]ProcessInfo {
	out := make(map[string]ProcessInfo, len(processes))
	for _, process := range processes {
		path := canonicalPathOrClean(process.PropsFile)
		if path == "" {
			continue
		}
		process.PropsFile = path
		out[path] = process
	}
	return out
}

func discoveryGroupKey(c DiscoveredCluster) string {
	if strings.TrimSpace(c.KafkaClusterID) != "" {
		return "cluster-id:" + strings.TrimSpace(c.KafkaClusterID)
	}
	if strings.TrimSpace(c.Name) != "" {
		return "name:" + strings.TrimSpace(c.Name)
	}
	return "install:" + c.InstallPath
}

func nodeIdentity(c DiscoveredCluster) string {
	return discoveryGroupKey(c) + "|node:" + stringInt(c.NodeID) + "|roles:" + c.ProcessRoles + "|config:" + c.PropsFile
}

func stringInt(value int) string {
	if value < 0 {
		return "unknown"
	}
	return strconv.Itoa(value)
}

func enrichClusterRecords(records []DiscoveredCluster, logger *slog.Logger) []DiscoveredCluster {
	type groupInfo struct {
		bootstrap   string
		security    string
		version     string
		clusterID   string
		brokerCount int
	}
	groups := map[string]*groupInfo{}

	for _, record := range records {
		key := discoveryGroupKey(record)
		info := groups[key]
		if info == nil {
			info = &groupInfo{}
			groups[key] = info
		}
		if record.KafkaClusterID != "" {
			info.clusterID = record.KafkaClusterID
		}
		if record.KafkaVersion != "" && record.KafkaVersion != "Unknown" {
			info.version = record.KafkaVersion
		}
		if roleContains(record.ProcessRoles, "broker") {
			info.brokerCount++
			if record.BootstrapServers != "" {
				info.bootstrap = record.BootstrapServers
			}
			if record.Security != "" {
				info.security = record.Security
			}
		}
	}

	var out []DiscoveredCluster
	seen := map[string]bool{}
	for _, record := range records {
		info := groups[discoveryGroupKey(record)]
		if info != nil {
			if record.BootstrapServers == "" {
				record.BootstrapServers = info.bootstrap
			}
			if record.Security == "" || roleContains(record.ProcessRoles, "controller") && !roleContains(record.ProcessRoles, "broker") {
				record.Security = firstNonBlank(info.security, record.Security, "UNKNOWN")
			}
			if record.KafkaVersion == "" || record.KafkaVersion == "Unknown" {
				record.KafkaVersion = firstNonBlank(info.version, "Unknown")
			}
			if record.KafkaClusterID == "" {
				record.KafkaClusterID = info.clusterID
			}
			if info.brokerCount > 0 {
				record.BrokerCount = info.brokerCount
			}
		}

		if record.BootstrapServers == "" {
			logger.Warn("Kafka node discovered but no broker bootstrap could be associated; report skipped",
				"config", record.PropsFile,
				"node_id", record.NodeID,
				"roles", record.ProcessRoles,
			)
			continue
		}
		identity := nodeIdentity(record)
		if seen[identity] {
			continue
		}
		seen[identity] = true
		out = append(out, record)
	}
	return out
}

func runDiscovery(ctx context.Context, client *APIClient, cfg RuntimeConfig, logger *slog.Logger) []DiscoveredCluster {
	runningProcesses := getRunningKafkaPropsFiles()
	procByConfig := processInfoByConfig(runningProcesses)

	var configPaths []string
	if cfg.DiscoveryPolicy != "filesystem-only" && len(procByConfig) > 0 {
		// Production rule: when Kafka is running, the running JVM is the source of
		// truth. Do NOT mix in stale/offline installations from filesystem scans.
		for path, process := range procByConfig {
			configPaths = append(configPaths, path)
			logger.Info("running Kafka server detected",
				"pid", process.Pid,
				"config", path,
				"service", process.SystemdUnit,
				"app_log_dir", process.AppLogDir,
			)
		}
		sort.Strings(configPaths)
	} else if cfg.DiscoveryPolicy == "running-only" {
		logger.Info("no running Kafka server process visible; running-only policy returns no active cluster", "scan_paths", cfg.ScanPaths)
		return nil
	} else {
		configPaths = findAllConfigProperties(cfg.ScanPaths)
		if len(configPaths) > 0 {
			logger.Warn("using offline filesystem inventory; records are not connectable", "policy", cfg.DiscoveryPolicy, "config_files", len(configPaths))
		}
	}

	if len(configPaths) == 0 {
		logger.Info("no Kafka broker/controller configuration found in this scan", "scan_paths", cfg.ScanPaths)
		return nil
	}

	var records []DiscoveredCluster
	for _, propsFile := range configPaths {
		canonical := canonicalPathOrClean(propsFile)
		process, running := procByConfig[canonical]

		if _, err := os.Open(canonical); err != nil {
			if running {
				logger.Error("running Kafka config is not readable by the agent user; refusing stale filesystem fallback",
					"config", canonical,
					"pid", process.Pid,
					"error", err,
				)
			}
			continue
		}

		dc := parseServerProperties(canonical, running, cfg.NodeName, cfg.Environment, cfg.ClusterName)
		if dc == nil {
			logger.Debug("Kafka properties file was not a usable broker/controller config", "config", canonical)
			continue
		}
		if running {
			dc.SystemdService = process.SystemdUnit
			if process.AppLogDir != "" {
				dc.LogDirs = process.AppLogDir
			}
		}
		dc.KafkaVersion = detectVersion(dc.InstallPath)
		dc.IsRunning = running
		records = append(records, *dc)
	}

	records = enrichClusterRecords(records, logger)
	logger.Info("Kafka discovery scan complete",
		"nodes", len(records),
		"running_server_processes", len(runningProcesses),
		"config_files", len(configPaths),
	)
	if len(records) == 0 {
		return nil
	}

	registered, failed := 0, 0
	for _, c := range records {
		logger.Info("reporting Kafka node",
			"cluster", c.Name,
			"cluster_id", c.KafkaClusterID,
			"node_id", c.NodeID,
			"roles", c.ProcessRoles,
			"bootstrap", c.BootstrapServers,
			"running", c.IsRunning,
			"install_path", c.InstallPath,
			"config", c.PropsFile,
			"data_dirs", c.DataDirs,
			"log_dirs", c.LogDirs,
			"hostname", c.Hostname,
		)
		hostMetrics := collectClusterMetrics(ctx, c)
		if registerCluster(ctx, client, cfg, c, hostMetrics, logger) {
			registered++
		} else {
			failed++
		}
	}
	logger.Info("registration cycle complete", "registered", registered, "failed", failed)
	return records
}
