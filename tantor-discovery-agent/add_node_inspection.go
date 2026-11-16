package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
)

const externalAddNodeInspectionSchema = "external-add-node-host/v1"
const externalAddNodeInspectionAPI = "/api/v1/ui/external-clusters/discovery/add-node-inspections"

// Host observation only. A zero count is not proof of clean Kafka storage,
// runtime compatibility, cluster health or permission to deploy a new process.
type addNodeHostObservation struct {
	SchemaVersion              string    `json:"schemaVersion"`
	AgentID                    string    `json:"agentId"`
	AgentVersion               string    `json:"agentVersion"`
	ObservedAt                 time.Time `json:"observedAt"`
	Addresses                  []string  `json:"addresses"`
	Root                       bool      `json:"root"`
	InspectionEnabled          bool      `json:"inspectionEnabled"`
	InventoryComplete          bool      `json:"inventoryComplete"`
	KafkaProcessCount          int       `json:"kafkaProcessCount"`
	KafkaUnitCount             int       `json:"kafkaUnitCount"`
	DiscoveredProcessCount     int       `json:"discoveredProcessCount"`
	ExecutionEnabled           bool      `json:"executionEnabled"`
	BrokerExecutionCapable     bool      `json:"brokerExecutionCapable"`
	ControllerExecutionCapable bool      `json:"controllerExecutionCapable"`
}

func validateAddNodeInspectionMode(cfg RuntimeConfig, uid int, goos string) error {
	if (cfg.EnableAddNodeExecution || cfg.EnableControllerAddition) && !cfg.EnableAddNodeInspection {
		return fmt.Errorf("broker execution requires explicit Add Node inspection")
	}
	if !cfg.EnableAddNodeInspection {
		return nil
	}
	if !cfg.EnableTasks || cfg.AuthType != "bearer" || uid != 0 || goos != "linux" {
		return fmt.Errorf("Add Node inspection requires Linux root, --enable-tasks and bearer authentication; mutation remains unavailable")
	}
	return nil
}

func observeAddNodeHost(cfg RuntimeConfig, records []DiscoveredCluster) addNodeHostObservation {
	r := addNodeHostObservation{SchemaVersion: externalAddNodeInspectionSchema, AgentID: cfg.HostID, AgentVersion: buildVersion, ObservedAt: time.Now().UTC(), Root: os.Geteuid() == 0, InspectionEnabled: cfg.EnableAddNodeInspection, DiscoveredProcessCount: len(records), Addresses: []string{}}
	r.BrokerExecutionCapable = cfg.EnableAddNodeExecution
	r.ControllerExecutionCapable = cfg.EnableControllerAddition
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return r
	}
	seen := map[string]bool{}
	for _, address := range addresses {
		ip, _, err := net.ParseCIDR(address.String())
		if err == nil && !ip.IsLoopback() && !ip.IsUnspecified() && !ip.IsLinkLocalUnicast() && !seen[ip.String()] {
			r.Addresses = append(r.Addresses, ip.String())
			seen[ip.String()] = true
		}
	}
	complete, processes, units := inspectAddNodeInventory("/proc", []string{"/etc/systemd/system", "/usr/lib/systemd/system", "/run/systemd/system"})
	r.InventoryComplete = complete
	r.KafkaProcessCount = processes
	r.KafkaUnitCount = units
	return r
}

func inspectAddNodeInventory(procRoot string, unitRoots []string) (bool, int, int) {
	processes, units := 0, 0
	complete := true
	entries, err := os.ReadDir(procRoot)
	if err != nil || len(entries) > 65536 {
		return false, 0, 0
	}
	processID := regexp.MustCompile(`^[0-9]+$`)
	for _, entry := range entries {
		if !processID.MatchString(entry.Name()) {
			continue
		}
		p := filepath.Join(procRoot, entry.Name(), "cmdline")
		info, err := os.Stat(p)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil || !info.Mode().IsRegular() {
			complete = false
			continue
		}
		// procfs reports size zero. Bound reads explicitly rather than trusting it.
		f, err := os.Open(p)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			complete = false
			continue
		}
		data, readErr := io.ReadAll(io.LimitReader(f, 65537))
		n := len(data)
		f.Close()
		if n > 65536 || readErr != nil {
			complete = false
			continue
		}
		for _, arg := range strings.Split(string(data[:n]), "\x00") {
			if arg == "kafka.Kafka" {
				processes++
				break
			}
		}
	}
	requiredRootFound := false
	kafkaUnit := regexp.MustCompile(`^(broker|controller|kafka|kafka-server|tantor-kafka[^/]*)\.service$`)
	for _, dir := range unitRoots {
		files, err := os.ReadDir(dir)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil || len(files) > 20000 {
			complete = false
			continue
		}
		requiredRootFound = true
		for _, file := range files {
			if kafkaUnit.MatchString(file.Name()) {
				units++
			}
		}
	}
	return complete && requiredRootFound, processes, units
}

func publishAddNodeInspection(ctx context.Context, client *APIClient, cfg RuntimeConfig, records []DiscoveredCluster) error {
	if err := validateAddNodeInspectionMode(cfg, os.Geteuid(), runtime.GOOS); err != nil {
		return err
	}
	if !cfg.EnableAddNodeInspection {
		return nil
	}
	// Register the host even before Kafka exists. The existing heartbeat endpoint
	// reports agent liveness; it does not create a discovered Kafka cluster/node.
	report := observeAddNodeHost(cfg, records)
	ips, _ := json.Marshal(report.Addresses)
	heartbeat := ExternalClusterPayload{IpAddresses: string(ips), HostID: cfg.HostID, AgentName: cfg.AgentName, Hostname: cfg.NodeName, AgentVersion: buildVersion, IsRunning: true, CanExecuteTasks: cfg.EnableTasks, CanDeployServices: cfg.EnableServiceDeployment && os.Geteuid() == 0}
	if _, err := client.DoJSON(ctx, http.MethodPost, client.endpoint("/api/v1/ui/external-clusters/discovery/heartbeat"), nil, heartbeat, nil); err != nil {
		return err
	}
	_, err := client.DoJSON(ctx, http.MethodPost, client.endpoint(externalAddNodeInspectionAPI), nil, report, nil)
	return err
}

func runAddNodeInspectionLoop(ctx context.Context, client *APIClient, cfg RuntimeConfig, store *ClusterStore, logger *slog.Logger) {
	ticker := time.NewTicker(cfg.MetricsInterval)
	defer ticker.Stop()
	for {
		if err := publishAddNodeInspection(ctx, client, cfg, store.Get()); err != nil {
			logger.Warn("Add Node host inspection could not be reported", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
