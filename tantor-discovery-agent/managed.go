package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	kc "io.translab/tantor-agent/pkg/kafkaconfig"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const managedAPI = "/api/v1/ui/external-clusters/discovery/kafka-tasks"

var managedMutationMu sync.Mutex
var managedBackupRegister = kc.Register

type managedTask struct {
	ID         string            `json:"id"`
	Command    string            `json:"command"`
	ClaimToken string            `json:"claimToken"`
	Parameters map[string]string `json:"parameters"`
}

func managedTarget(t managedTask, records []DiscoveredCluster) (DiscoveredCluster, error) {
	for _, r := range records {
		if strconv.Itoa(r.NodeID) != t.Parameters["node_id"] {
			continue
		}
		if t.Parameters["kafka_cluster_id"] == "" || r.KafkaClusterID != t.Parameters["kafka_cluster_id"] {
			continue
		}
		if err := validatePathSafety(t.Parameters["config_path"], r); err != nil {
			continue
		}
		if t.Parameters["service_name"] != "" && t.Parameters["service_name"] != r.SystemdService {
			continue
		}
		content, err := os.ReadFile(r.PropsFile)
		if err != nil {
			continue
		}
		props := parseProperties(string(content))
		if parseIntDefault(firstNonBlank(props["node.id"], props["broker.id"]), -1) != r.NodeID {
			continue
		}
		actualIdentity := firstNonBlank(props["cluster.id"], readClusterIDFromLogs(strings.Join(nonBlankStrings(props["log.dirs"], props["metadata.log.dir"]), ",")))
		if actualIdentity != t.Parameters["kafka_cluster_id"] {
			continue
		}
		return r, nil
	}
	return DiscoveredCluster{}, fmt.Errorf("task does not match a locally discovered Kafka process and configuration")
}
func executeManagedTask(ctx context.Context, cfg RuntimeConfig, records []DiscoveredCluster, t managedTask) serviceResult {
	managedMutationMu.Lock()
	defer managedMutationMu.Unlock()
	r, err := managedTarget(t, records)
	if err != nil {
		return serviceResult{"FAILED", err.Error()}
	}
	output, err := runManagedTask(ctx, cfg, r, t)
	if err != nil {
		return serviceResult{"FAILED", output + "\n" + err.Error()}
	}
	return serviceResult{"SUCCESS", output}
}
func runManagedTask(ctx context.Context, cfg RuntimeConfig, r DiscoveredCluster, t managedTask) (string, error) {
	p := t.Parameters
	path, pathErr := canonicalExistingPath(r.PropsFile)
	if pathErr != nil {
		return "", pathErr
	}
	if t.Command == "VERIFY_KAFKA_SERVICE" || t.Command == "RESTART_SERVICE" {
		unit := p["service_name"]
		if err := validateDiscoveredService(unit, r); err != nil {
			return "", err
		}
		binding, bindErr := exec.CommandContext(ctx, "systemctl", "show", unit, "--property=ExecStart", "--value").Output()
		pattern := regexp.MustCompile(`(?:^|[\s="'])` + regexp.QuoteMeta(r.PropsFile) + `(?:[\s;"'}]|$)`)
		if bindErr != nil || !pattern.Match(binding) {
			return "", fmt.Errorf("service configuration binding changed; refresh discovery before restarting")
		}

		if t.Command == "RESTART_SERVICE" {
			if err := executeRestartService(ctx, AgentTask{ServiceName: unit}, r, cfg); err != nil {
				return "", err
			}
		}
		if err := validateDiscoveredService(unit, r); err != nil {
			return "", err
		}
		out, err := exec.CommandContext(ctx, "systemctl", "show", unit, "--property=ActiveState", "--property=MainPID", "--property=ExecMainStartTimestampMonotonic").Output()
		if err != nil {
			return "", err
		}
		values := map[string]string{}
		for _, line := range strings.Split(string(out), "\n") {
			k, v, ok := strings.Cut(line, "=")
			if ok {
				values[k] = v
			}
		}
		if values["ActiveState"] == "active" && (values["MainPID"] == "0" || values["MainPID"] == "" || values["ExecMainStartTimestampMonotonic"] == "") {
			return "", fmt.Errorf("active service has no process identity")
		}
		data, _ := json.Marshal(map[string]string{"state": values["ActiveState"], "pid": values["MainPID"], "started": values["ExecMainStartTimestampMonotonic"]})
		return string(data), nil
	}
	file, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return "", err
	}
	defer file.Close()
	if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		return "", err
	}
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	original, err := io.ReadAll(file)
	if err != nil {
		return "", err
	}
	props, err := kc.Read(string(original))
	if err != nil {
		return "", err
	}
	if t.Command == "READ_KAFKA_PROPERTIES" {
		data, _ := json.Marshal(map[string]interface{}{"properties": props})
		return string(data), nil
	}
	if t.Command == "READ_KAFKA_PROPERTY" {
		v, found := props[p["config_key"]]
		data, _ := json.Marshal(map[string]interface{}{"value": v, "found": found})
		return string(data), nil
	}
	if !cfg.AllowConfigWrite || os.Geteuid() != 0 {
		return "", fmt.Errorf("managed configuration backups and writes require root and allow-config-write")
	}
	var changes, expected map[string]*string
	var updated string
	var changed bool
	if t.Command == "PATCH_KAFKA_CONFIG" {
		if json.Unmarshal([]byte(p["config_changes_json"]), &changes) != nil || len(changes) == 0 {
			return "", fmt.Errorf("invalid property changes")
		}
		if json.Unmarshal([]byte(p["expected_values_json"]), &expected) != nil {
			return "", fmt.Errorf("invalid expected values")
		}
		for k, v := range changes {
			if _, ok := expected[k]; !ok {
				return "", fmt.Errorf("missing expected value for %s", k)
			}
			if v != nil && (len(*v) > 4096 || strings.ContainsAny(*v, "\r\n\x00")) {
				return "", fmt.Errorf("invalid value")
			}
		}
		updated, changed, err = kc.Patch(string(original), changes, expected)
		if err != nil {
			return "", err
		}
	} else if t.Command != "BACKUP_KAFKA_CONFIG" {
		return "", fmt.Errorf("unsupported managed command")
	}
	snapshot := p["dynamic_snapshot_json"]
	if t.Command == "BACKUP_KAFKA_CONFIG" {
		var decoded map[string]interface{}
		if len(snapshot) > 16384 || json.Unmarshal([]byte(snapshot), &decoded) != nil || decoded["mode"] != "DYNAMIC" || decoded["key"] != p["config_key"] {
			return "", fmt.Errorf("invalid dynamic snapshot")
		}
	}
	directory := filepath.Join(filepath.Dir(path), "config-backup", filepath.Base(path))
	if err = os.MkdirAll(directory, 0700); err != nil {
		return "", err
	}
	if err = os.Chmod(directory, 0700); err != nil {
		return "", err
	}
	backup, err := kc.Reserve(directory, path, time.Now())
	if err != nil {
		return "", err
	}
	if err = os.WriteFile(backup, original, 0600); err != nil {
		return "", err
	}
	if err = managedBackupRegister(ctx, directory); err != nil {
		return "", err
	}
	if snapshot != "" {
		if err = os.WriteFile(backup+".dynamic.json", []byte(snapshot), 0600); err != nil {
			return "", err
		}
	}
	logs := "Configuration backup (7-day retention): " + backup
	if t.Command == "PATCH_KAFKA_CONFIG" && changed {
		if err = kc.Write(path, []byte(updated)); err != nil {
			return logs, err
		}
	}
	if p["restart"] == "true" {
		if err = executeRestartService(ctx, AgentTask{ServiceName: p["service_name"]}, r, cfg); err != nil {
			return logs, err
		}
	}
	return logs, nil
}
func runManagedPoller(ctx context.Context, client *APIClient, cfg RuntimeConfig, store *ClusterStore, logger *slog.Logger) {
	if cfg.AuthType != "bearer" {
		logger.Warn("Managed Kafka tasks require configured bearer authentication with backend ADMIN task access")
		return
	}
	query := url.Values{"agentId": {cfg.HostID}}
	ticker := time.NewTicker(cfg.TaskPollInterval)
	defer ticker.Stop()
	directory := filepath.Join(cfg.ServiceStateDir, "kafka-tasks")
	if err := os.MkdirAll(directory, 0700); err != nil {
		logger.Error("cannot create managed task journal", "error", err)
		return
	}
	sweep := func() {
		if os.Geteuid() == 0 {
			if err := kc.Sweep(time.Now()); err != nil {
				logger.Warn("backup retention failed", "error", err)
			}
		}
	}
	sweep()
	lastSweep := time.Now()
	known := map[string]DiscoveredCluster{}
	inventoryPath := filepath.Join(directory, "inventory.json")
	if data, err := os.ReadFile(inventoryPath); err == nil {
		_ = json.Unmarshal(data, &known)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, r := range store.Get() {
			known[r.PropsFile] = r
		}
		if data, err := json.Marshal(known); err == nil {
			if os.WriteFile(inventoryPath+".tmp", data, 0600) == nil {
				_ = os.Rename(inventoryPath+".tmp", inventoryPath)
			}
		}
		records := []DiscoveredCluster{}
		for _, r := range known {
			records = append(records, r)
		}
		if time.Since(lastSweep) > time.Hour {
			sweep()
			lastSweep = time.Now()
		}
		var t managedTask
		if _, err := client.DoJSON(ctx, http.MethodPost, client.baseURL+managedAPI+"/claim", query, nil, &t); err != nil {
			logger.Debug("managed task poll failed", "error", err)
			continue
		}
		if t.ID == "" {
			continue
		}
		if !serviceUUID.MatchString(t.ID) {
			continue
		}
		path := filepath.Join(directory, t.ID+".json")
		result := serviceResult{}
		if b, err := os.ReadFile(path); err == nil {
			if json.Unmarshal(b, &result) != nil {
				result = serviceResult{"FAILED", "Invalid task journal; inspect node"}
			}
		} else {
			result = serviceResult{"FAILED", "Agent interrupted; inspect process and backup before retry"}
			b, _ := json.Marshal(result)
			if err = os.WriteFile(path, b, 0600); err != nil {
				continue
			}
			taskCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
			result = executeManagedTask(taskCtx, cfg, records, t)
			cancel()
			b, _ = json.Marshal(result)
			if os.WriteFile(path+".tmp", b, 0600) != nil {
				continue
			}
			if os.Rename(path+".tmp", path) != nil {
				continue
			}
		}
		values := map[string]string{"status": result.Status, "message": result.Message, "claimToken": t.ClaimToken}
		if _, err := client.DoJSON(ctx, http.MethodPost, client.baseURL+managedAPI+"/"+t.ID+"/complete", query, values, nil); err != nil {
			logger.Warn("managed task report pending", "error", err)
		}
	}
}
