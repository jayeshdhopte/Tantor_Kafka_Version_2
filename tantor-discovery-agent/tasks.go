package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

type AgentTask struct {
	TaskID         string            `json:"taskId"`
	Task           string            `json:"task"`
	ConfigFilePath string            `json:"configFilePath"`
	BackupDirPath  string            `json:"backupDirPath"`
	BackupFilePath string            `json:"backupFilePath"`
	ConfigChanges  map[string]string `json:"configChanges"`
	ServiceName    string            `json:"serviceName"`
}

type AgentTaskResult struct {
	TaskID  string            `json:"taskId"`
	Status  string            `json:"status"`
	Message string            `json:"message"`
	Data    map[string]string `json:"data"`
}

func taskGroupKey(cluster DiscoveredCluster, hostname string) string {
	return cluster.Name + "|" + cluster.BootstrapServers + "|" + hostname
}

func chooseTaskTarget(task AgentTask, records []DiscoveredCluster) *DiscoveredCluster {
	if len(records) == 0 {
		return nil
	}
	if strings.TrimSpace(task.ConfigFilePath) != "" {
		requested, err := canonicalExistingPath(task.ConfigFilePath)
		if err == nil {
			for i := range records {
				managed, err := canonicalExistingPath(records[i].PropsFile)
				if err == nil && requested == managed {
					return &records[i]
				}
			}
		}
	}
	if strings.TrimSpace(task.ServiceName) != "" {
		for i := range records {
			if records[i].SystemdService == task.ServiceName {
				return &records[i]
			}
		}
	}
	// Prefer a broker node for cluster-level tasks when no exact target is supplied.
	for i := range records {
		if roleContains(records[i].ProcessRoles, "broker") {
			return &records[i]
		}
	}
	return &records[0]
}

func executePolledTask(ctx context.Context, client *APIClient, cfg RuntimeConfig, representative DiscoveredCluster, records []DiscoveredCluster, task AgentTask, logger *slog.Logger) {
	result := AgentTaskResult{TaskID: task.TaskID, Status: "SUCCESS", Data: make(map[string]string)}
	target := chooseTaskTarget(task, records)
	if strings.TrimSpace(task.TaskID) == "" {
		result.Status = "FAILED"
		result.Message = "taskId is required"
	} else if target == nil {
		result.Status = "FAILED"
		result.Message = "no discovered Kafka node matches this task"
	} else {
		logger.Info("task received", "task", task.Task, "task_id", task.TaskID, "cluster", representative.Name, "target_config", target.PropsFile, "target_node_id", target.NodeID)
		switch task.Task {
		case "backup_file":
			path, err := executeBackupFile(task, *target, cfg)
			if err != nil {
				result.Status, result.Message = "FAILED", err.Error()
			} else {
				result.Data["backupFilePath"] = path
			}
		case "restore_backup":
			if err := executeRestoreBackup(task, *target, cfg); err != nil {
				result.Status, result.Message = "FAILED", err.Error()
			}
		case "read_config":
			props, err := executeReadConfig(task.ConfigFilePath, *target)
			if err != nil {
				result.Status, result.Message = "FAILED", err.Error()
			} else {
				for k, v := range props {
					result.Data[k] = v
				}
			}
		case "write_config":
			backupPath, err := executeWriteConfig(task, *target, cfg)
			if err != nil {
				result.Status, result.Message = "FAILED", err.Error()
			} else {
				result.Data["automaticBackupFilePath"] = backupPath
			}
		case "restart_service":
			if err := executeRestartService(ctx, task, *target, cfg); err != nil {
				result.Status, result.Message = "FAILED", err.Error()
			}
		case "service_status":
			active, err := executeServiceStatus(ctx, task.ServiceName, *target, cfg)
			if err != nil {
				result.Status, result.Message = "FAILED", err.Error()
			} else {
				result.Data["active"] = fmt.Sprintf("%t", active)
			}
		default:
			result.Status = "FAILED"
			result.Message = "unsupported task: " + task.Task
		}
	}

	if err := completeTaskWithResult(ctx, client, representative, cfg.NodeName, result); err != nil {
		logger.Error("task completion report failed", "task_id", task.TaskID, "cluster", representative.Name, "error", err)
		return
	}
	logger.Info("task completed", "task", task.Task, "task_id", task.TaskID, "status", result.Status)
}

func pollForTaskGroup(ctx context.Context, client *APIClient, cfg RuntimeConfig, records []DiscoveredCluster, logger *slog.Logger) {
	if len(records) == 0 {
		return
	}
	representative := records[0]
	for _, record := range records {
		if roleContains(record.ProcessRoles, "broker") {
			representative = record
			break
		}
	}
	query := url.Values{}
	query.Set("hostname", cfg.NodeName)
	query.Set("bootstrap", representative.BootstrapServers)

	var task AgentTask
	status, err := client.DoJSON(ctx, http.MethodGet, client.endpoint(externalAgentPath(representative.Name, "/tasks")), query, nil, &task)
	if err != nil {
		if apiErr, ok := err.(*APIError); ok && apiErr.StatusCode == http.StatusNotFound {
			return
		}
		logger.Debug("task poll failed", "cluster", representative.Name, "status", status, "error", err)
		return
	}
	if task.Task == "" || strings.EqualFold(task.Task, "NONE") {
		return
	}
	executePolledTask(ctx, client, cfg, representative, records, task, logger)
}

func runTaskPoller(ctx context.Context, client *APIClient, cfg RuntimeConfig, store *ClusterStore, logger *slog.Logger) {
	ticker := time.NewTicker(cfg.TaskPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			groups := map[string][]DiscoveredCluster{}
			for _, cluster := range store.Get() {
				key := taskGroupKey(cluster, cfg.NodeName)
				groups[key] = append(groups[key], cluster)
			}
			for _, records := range groups {
				pollForTaskGroup(ctx, client, cfg, records, logger)
			}
		}
	}
}

func canonicalExistingPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", err
	}
	return filepath.Clean(resolved), nil
}

func validatePathSafety(path string, cluster DiscoveredCluster) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("configFilePath cannot be empty")
	}
	requested, err := canonicalExistingPath(path)
	if err != nil {
		return fmt.Errorf("resolve requested config path: %w", err)
	}
	managed, err := canonicalExistingPath(cluster.PropsFile)
	if err != nil {
		return fmt.Errorf("resolve managed config path: %w", err)
	}
	if requested != managed {
		return fmt.Errorf("security violation: %s is not the discovered config file for this node", path)
	}
	return nil
}

func isAllowedService(service string, allowed []string) bool {
	for _, item := range allowed {
		if service == item {
			return true
		}
	}
	return false
}

func validateDiscoveredService(serviceName string, cluster DiscoveredCluster) error {
	unitRE := regexp.MustCompile(`^[A-Za-z0-9_.@:-]+\.service$`)
	if !unitRE.MatchString(serviceName) {
		return fmt.Errorf("invalid systemd service name: %q", serviceName)
	}
	if cluster.SystemdService == "" {
		return fmt.Errorf("no running systemd service was discovered for this Kafka node")
	}
	if serviceName != cluster.SystemdService {
		return fmt.Errorf("security violation: service %s does not match discovered service %s", serviceName, cluster.SystemdService)
	}
	return nil
}

func validateRestartServiceSafety(serviceName string, cluster DiscoveredCluster, cfg RuntimeConfig) error {
	if err := validateDiscoveredService(serviceName, cluster); err != nil {
		return err
	}
	if !isAllowedService(serviceName, cfg.AllowedServices) {
		return fmt.Errorf("security violation: service %s is not in the local restart allowlist", serviceName)
	}
	return nil
}

func pathWithinRoot(root, candidate string) (string, error) {
	if candidate == "" {
		return "", fmt.Errorf("path cannot be empty")
	}
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(root, candidate)
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	candidateAbs, err := filepath.Abs(candidate)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(rootAbs, candidateAbs)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("security violation: backup path %s is outside %s", candidate, root)
	}
	return filepath.Clean(candidateAbs), nil
}

func safeBackupTarget(cluster DiscoveredCluster, cfg RuntimeConfig) (string, error) {
	backupDir, err := pathWithinRoot(cfg.BackupRoot, sanitizeName(cluster.Name))
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(backupDir, 0750); err != nil {
		return "", fmt.Errorf("create backup directory: %w", err)
	}
	name := filepath.Base(cluster.PropsFile) + "." + time.Now().UTC().Format("20060102T150405.000000000Z") + ".bak"
	return filepath.Join(backupDir, name), nil
}

func writeBackupFile(sourcePath, destination string, metadata AgentTask) error {
	src, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("open source config: %w", err)
	}
	defer src.Close()

	dst, err := os.OpenFile(destination, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("create backup file: %w", err)
	}
	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		return fmt.Errorf("copy backup: %w", err)
	}
	if err := dst.Sync(); err != nil {
		_ = dst.Close()
		return fmt.Errorf("sync backup: %w", err)
	}
	if err := dst.Close(); err != nil {
		return err
	}

	metaBytes, _ := json.MarshalIndent(metadata, "", "  ")
	if err := os.WriteFile(destination+".meta.json", metaBytes, 0600); err != nil {
		return fmt.Errorf("write backup metadata: %w", err)
	}
	return nil
}

func executeBackupFile(task AgentTask, cluster DiscoveredCluster, cfg RuntimeConfig) (string, error) {
	configPath := firstNonBlank(task.ConfigFilePath, cluster.PropsFile)
	if err := validatePathSafety(configPath, cluster); err != nil {
		return "", err
	}
	backupPath, err := safeBackupTarget(cluster, cfg)
	if err != nil {
		return "", err
	}
	if err := writeBackupFile(configPath, backupPath, task); err != nil {
		return "", err
	}
	return backupPath, nil
}

func executeRestoreBackup(task AgentTask, cluster DiscoveredCluster, cfg RuntimeConfig) error {
	if !cfg.AllowConfigWrite {
		return fmt.Errorf("config write/restore capability is disabled on this agent")
	}
	configPath := firstNonBlank(task.ConfigFilePath, cluster.PropsFile)
	if err := validatePathSafety(configPath, cluster); err != nil {
		return err
	}
	backupPath, err := pathWithinRoot(cfg.BackupRoot, task.BackupFilePath)
	if err != nil {
		return err
	}
	canonicalRoot, err := canonicalExistingPath(cfg.BackupRoot)
	if err != nil {
		return fmt.Errorf("resolve backup root: %w", err)
	}
	canonicalBackup, err := canonicalExistingPath(backupPath)
	if err != nil {
		return fmt.Errorf("resolve backup file: %w", err)
	}
	rel, err := filepath.Rel(canonicalRoot, canonicalBackup)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("security violation: resolved backup file is outside the backup root")
	}
	backupBytes, err := os.ReadFile(canonicalBackup)
	if err != nil {
		return fmt.Errorf("read backup: %w", err)
	}
	return overwriteLockedFile(configPath, backupBytes)
}

func executeReadConfig(path string, cluster DiscoveredCluster) (map[string]string, error) {
	path = firstNonBlank(path, cluster.PropsFile)
	if err := validatePathSafety(path, cluster); err != nil {
		return nil, err
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return parseProperties(string(content)), nil
}

func validateConfigChanges(changes map[string]string) error {
	keyRE := regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
	for key, value := range changes {
		if !keyRE.MatchString(strings.TrimSpace(key)) {
			return fmt.Errorf("invalid Kafka property key: %q", key)
		}
		if strings.ContainsAny(value, "\r\n\x00") {
			return fmt.Errorf("invalid newline or NUL in value for property %s", key)
		}
	}
	return nil
}

func applyConfigChanges(content string, changes map[string]string) string {
	lines := strings.Split(content, "\n")
	updated := make(map[string]bool, len(changes))
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "!") {
			continue
		}
		idx := strings.IndexAny(trimmed, "=:")
		if idx < 0 {
			continue
		}
		key := strings.TrimSpace(trimmed[:idx])
		value, ok := changes[key]
		if !ok {
			continue
		}
		updated[key] = true
		if value == "" {
			lines[i] = "# removed by Tantor agent: " + line
		} else {
			lines[i] = key + "=" + value
		}
	}
	for key, value := range changes {
		if !updated[key] && value != "" {
			lines = append(lines, key+"="+value)
		}
	}
	return strings.Join(lines, "\n")
}

func overwriteLockedFile(path string, content []byte) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open config for write: %w", err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock config: %w", err)
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN) //nolint:errcheck
	if err := f.Truncate(0); err != nil {
		return fmt.Errorf("truncate config: %w", err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	if _, err := f.Write(content); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync config: %w", err)
	}
	return nil
}

func executeWriteConfig(task AgentTask, cluster DiscoveredCluster, cfg RuntimeConfig) (string, error) {
	if !cfg.AllowConfigWrite {
		return "", fmt.Errorf("config write capability is disabled on this agent")
	}
	if len(task.ConfigChanges) == 0 {
		return "", fmt.Errorf("configChanges cannot be empty")
	}
	if err := validateConfigChanges(task.ConfigChanges); err != nil {
		return "", err
	}
	configPath := firstNonBlank(task.ConfigFilePath, cluster.PropsFile)
	if err := validatePathSafety(configPath, cluster); err != nil {
		return "", err
	}

	f, err := os.OpenFile(configPath, os.O_RDWR, 0)
	if err != nil {
		return "", fmt.Errorf("open config for update: %w", err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return "", fmt.Errorf("lock config: %w", err)
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN) //nolint:errcheck

	original, err := io.ReadAll(f)
	if err != nil {
		return "", fmt.Errorf("read locked config: %w", err)
	}
	backupPath, err := safeBackupTarget(cluster, cfg)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(backupPath, original, 0600); err != nil {
		return "", fmt.Errorf("automatic backup before write failed: %w", err)
	}

	updated := []byte(applyConfigChanges(string(original), task.ConfigChanges))
	if err := f.Truncate(0); err != nil {
		return "", fmt.Errorf("truncate config: %w", err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		return "", err
	}
	if _, err := f.Write(updated); err != nil {
		return "", fmt.Errorf("write config: %w", err)
	}
	if err := f.Sync(); err != nil {
		return "", fmt.Errorf("sync config: %w", err)
	}
	return backupPath, nil
}

func executeRestartService(parent context.Context, task AgentTask, cluster DiscoveredCluster, cfg RuntimeConfig) error {
	if !cfg.AllowServiceRestart {
		return fmt.Errorf("service restart capability is disabled on this agent")
	}
	if err := validateRestartServiceSafety(task.ServiceName, cluster, cfg); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 60*time.Second)
	defer cancel()
	var cmd *exec.Cmd
	if cfg.RestartWithSudo {
		cmd = exec.CommandContext(ctx, "sudo", "-n", "systemctl", "restart", task.ServiceName)
	} else {
		cmd = exec.CommandContext(ctx, "systemctl", "restart", task.ServiceName)
	}
	output, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("restart timed out")
	}
	if err != nil {
		return fmt.Errorf("restart failed: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func executeServiceStatus(parent context.Context, serviceName string, cluster DiscoveredCluster, cfg RuntimeConfig) (bool, error) {
	if err := validateDiscoveredService(serviceName, cluster); err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	err := exec.CommandContext(ctx, "systemctl", "is-active", "--quiet", serviceName).Run()
	if err == nil {
		return true, nil
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 3 {
		return false, nil
	}
	return false, err
}
