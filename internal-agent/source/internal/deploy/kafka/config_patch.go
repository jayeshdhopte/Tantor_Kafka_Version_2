package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"io.translab/tantor-agent/internal/taskvalidate"
	"io.translab/tantor-agent/pkg/api"
)

var patchKeyPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9._-]{0,127}$`)

var protectedPatchKeys = map[string]bool{
	"node.id": true, "broker.id": true, "process.roles": true,
	"log.dirs": true, "metadata.log.dir": true, "listeners": true,
	"advertised.listeners": true, "controller.quorum.voters": true,
	"controller.quorum.bootstrap.servers": true, "inter.broker.listener.name": true,
	"cluster.id": true, "zookeeper.connect": true,
}

// PatchConfig changes only requested Kafka properties. The original file is
// backed up before mutation; a failed restart leaves it available for an
// explicit, operator-controlled recovery.
func (d *Deployer) PatchConfig(ctx context.Context, task *api.Task) (string, error) {
	if os.Geteuid() != 0 {
		return "", fmt.Errorf("targeted Kafka config patch requires a root agent")
	}
	configPath, err := validatedPatchPath(task)
	if err != nil {
		return "", err
	}
	changes := make(map[string]*string)
	expected := make(map[string]*string)
	if err := json.Unmarshal([]byte(task.Parameters["config_changes_json"]), &changes); err != nil || len(changes) == 0 {
		return "", fmt.Errorf("invalid config_changes_json")
	}
	if err := json.Unmarshal([]byte(task.Parameters["expected_values_json"]), &expected); err != nil {
		return "", fmt.Errorf("invalid expected_values_json: %w", err)
	}
	for key, value := range changes {
		if err := validatePatchKey(key); err != nil {
			return "", err
		}
		if _, ok := expected[key]; !ok {
			return "", fmt.Errorf("missing expected value for %s", key)
		}
		if value != nil && (len(*value) > 4096 || strings.ContainsAny(*value, "\r\n\x00")) {
			return "", fmt.Errorf("invalid value for %s", key)
		}
	}
	original, err := os.ReadFile(configPath)
	if err != nil {
		return "", fmt.Errorf("read active config: %w", err)
	}
	updated, changed, err := patchProperties(string(original), changes, expected)
	if err != nil {
		return "", err
	}
	var logs strings.Builder
	backup, err := d.backupConfig(ctx, task, configPath)
	if err != nil {
		return "", err
	}
	logs.WriteString("Rollback checkpoint (7-day retention): " + backup + "\n")
	if changed {
		if err := writePatchAtomically(configPath, []byte(updated)); err != nil {
			return logs.String(), fmt.Errorf("write config patch: %w", err)
		}
		logs.WriteString("Requested properties patched without replacing unrelated settings\n")
	} else {
		logs.WriteString("Requested properties already match the active file\n")
	}
	if task.Parameters["restart"] == "true" {
		service := serviceNameForTask(task)
		if _, errOut, err := d.exec.RunSudo(ctx, "systemctl", "restart", service); err != nil {
			return logs.String(), fmt.Errorf("restart %s failed; inspect checkpoint before recovery: %w (%s)", service, err, errOut)
		}
		if _, errOut, err := d.exec.RunSudo(ctx, "systemctl", "is-active", "--quiet", service); err != nil {
			return logs.String(), fmt.Errorf("%s did not become active: %w (%s)", service, err, errOut)
		}
		logs.WriteString("Service " + service + " restarted and active\n")
	}
	verified, err := os.ReadFile(configPath)
	if err != nil {
		return logs.String(), fmt.Errorf("read patched config for verification: %w", err)
	}
	_, differs, err := patchProperties(string(verified), changes, changes)
	if err != nil || differs {
		return logs.String(), fmt.Errorf("active config differs from requested properties: %v", err)
	}
	logs.WriteString("Active config values verified\n")
	return logs.String(), nil
}

// ReadConfigProperty reports only the requested non-sensitive property.
func (d *Deployer) ReadConfigProperty(_ context.Context, task *api.Task) (string, error) {
	key := strings.TrimSpace(task.Parameters["config_key"])
	if err := validatePatchKey(key); err != nil {
		return "", err
	}
	path, err := validatedPatchPath(task)
	if err != nil {
		return "", err
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read active config: %w", err)
	}
	lines := strings.Split(string(content), "\n")
	var found bool
	var value string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "!") {
			continue
		}
		separator := strings.IndexAny(trimmed, "=:")
		if separator < 0 || strings.TrimSpace(trimmed[:separator]) != key {
			continue
		}
		if found {
			return "", fmt.Errorf("duplicate active property %s", key)
		}
		found = true
		value = strings.TrimSpace(trimmed[separator+1:])
	}
	result, err := json.Marshal(map[string]interface{}{"found": found, "value": value})
	if err != nil {
		return "", err
	}
	return string(result), nil
}

// ReadConfigProperties returns editable properties from the active node file.
// Protected and sensitive keys are omitted before the result leaves the agent.
func (d *Deployer) ReadConfigProperties(_ context.Context, task *api.Task) (string, error) {
	path, err := validatedPatchPath(task)
	if err != nil {
		return "", err
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read active config: %w", err)
	}
	properties, err := parseReadableProperties(string(content))
	if err != nil {
		return "", err
	}
	result, err := json.Marshal(map[string]interface{}{"properties": properties})
	if err != nil {
		return "", err
	}
	return string(result), nil
}

func parseReadableProperties(content string) (map[string]string, error) {
	properties := make(map[string]string)
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "!") {
			continue
		}
		separator := strings.IndexAny(trimmed, "=:")
		if separator < 0 {
			continue
		}
		key := strings.TrimSpace(trimmed[:separator])
		if validatePatchKey(key) != nil {
			continue
		}
		if _, duplicate := properties[key]; duplicate {
			return nil, fmt.Errorf("duplicate active property %s", key)
		}
		properties[key] = strings.TrimSpace(trimmed[separator+1:])
	}
	return properties, nil
}

func validatePatchKey(key string) error {
	lower := strings.ToLower(key)
	if !patchKeyPattern.MatchString(key) || protectedPatchKeys[lower] ||
		strings.Contains(lower, "password") || strings.Contains(lower, "secret") ||
		strings.Contains(lower, "token") || strings.Contains(lower, "jaas") ||
		strings.Contains(lower, "credential") || strings.Contains(lower, "keystore") ||
		strings.Contains(lower, "truststore") {
		return fmt.Errorf("property %q cannot be inspected or patched", key)
	}
	return nil
}

func validatedPatchPath(task *api.Task) (string, error) {
	requested := strings.TrimSpace(task.Parameters["config_path"])
	if _, err := taskvalidate.ApprovedPath(requested); err != nil {
		return "", err
	}
	base := filepath.Base(requested)
	if base != "broker.properties" && base != "controller.properties" && base != "server.properties" {
		return "", fmt.Errorf("unexpected Kafka config filename %q", base)
	}
	active := resolveKafkaInstallPaths(task).ActiveDir
	root, err := filepath.EvalSymlinks(filepath.Join(active, "config"))
	if err != nil {
		return "", fmt.Errorf("resolve active Kafka config directory: %w", err)
	}
	actual, err := filepath.EvalSymlinks(requested)
	if err != nil {
		return "", fmt.Errorf("resolve active Kafka config file: %w", err)
	}
	if !strings.HasPrefix(actual, root+string(os.PathSeparator)) {
		return "", fmt.Errorf("Kafka config file is outside the active installation")
	}
	return requested, nil
}

func patchProperties(content string, changes, expected map[string]*string) (string, bool, error) {
	lines := strings.Split(content, "\n")
	positions := make(map[string]int)
	current := make(map[string]*string)
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "!") {
			continue
		}
		separator := strings.IndexAny(trimmed, "=:")
		if separator < 0 {
			continue
		}
		key := strings.TrimSpace(trimmed[:separator])
		if _, relevant := changes[key]; !relevant {
			continue
		}
		if _, duplicate := positions[key]; duplicate {
			return "", false, fmt.Errorf("duplicate active property %s", key)
		}
		value := strings.TrimSpace(trimmed[separator+1:])
		positions[key] = i
		current[key] = &value
	}
	changed := false
	removed := make(map[int]bool)
	for key, next := range changes {
		prior := current[key]
		want := expected[key]
		if !samePatchValue(prior, want) && !samePatchValue(prior, next) {
			return "", false, fmt.Errorf("property %s changed since preview; refresh the plan", key)
		}
		if samePatchValue(prior, next) {
			continue
		}
		changed = true
		if index, exists := positions[key]; exists {
			if next == nil {
				removed[index] = true
			} else {
				lines[index] = key + "=" + *next
			}
		} else if next != nil {
			lines = append(lines, key+"="+*next)
		}
	}
	kept := make([]string, 0, len(lines))
	for index, line := range lines {
		if !removed[index] {
			kept = append(kept, line)
		}
	}
	output := strings.Join(kept, "\n")
	if changed && !strings.HasSuffix(output, "\n") {
		output += "\n"
	}
	return output, changed, nil
}

func samePatchValue(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func writePatchAtomically(path string, content []byte) error {
	stat, err := os.Stat(path)
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".tantor-patch-*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	defer temp.Close()
	if err := temp.Chmod(stat.Mode().Perm()); err != nil {
		return err
	}
	if err := preservePatchOwner(temp, stat); err != nil {
		return err
	}
	if _, err := temp.Write(content); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(temp.Name(), path)
}

// BackupDynamicConfig checkpoints the disk config and the requested property's
// live Kafka state before the backend performs any Admin API mutation.
func (d *Deployer) BackupDynamicConfig(ctx context.Context, task *api.Task) (string, error) {
	if os.Geteuid() != 0 {
		return "", fmt.Errorf("dynamic config backup requires a root agent")
	}
	configPath, err := validatedPatchPath(task)
	if err != nil {
		return "", err
	}
	key := task.Parameters["config_key"]
	if err := validatePatchKey(key); err != nil {
		return "", err
	}
	snapshot := task.Parameters["dynamic_snapshot_json"]
	var decoded map[string]interface{}
	if len(snapshot) > 16384 || json.Unmarshal([]byte(snapshot), &decoded) != nil || decoded["key"] != key || decoded["mode"] != "DYNAMIC" {
		return "", fmt.Errorf("invalid dynamic configuration snapshot")
	}
	backup, err := d.backupConfig(ctx, task, configPath)
	if err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp("", "tantor-dynamic-backup-")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	_, err = tmp.WriteString(snapshot + "\n")
	closeErr := tmp.Close()
	if err != nil {
		return "", err
	}
	if closeErr != nil {
		return "", closeErr
	}
	dynamicPath := backup + ".dynamic.json"
	if _, detail, err := d.exec.RunSudo(ctx, "install", "-m", "0600", "--", tmp.Name(), dynamicPath); err != nil {
		return "", fmt.Errorf("save dynamic checkpoint: %w (%s)", err, detail)
	}
	return "Configuration backup (7-day retention): " + backup + "\nLive dynamic property checkpoint: " + dynamicPath, nil
}
