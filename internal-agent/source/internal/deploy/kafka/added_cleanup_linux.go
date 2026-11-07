//go:build linux

package kafka

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"io.translab/tantor-agent/internal/taskvalidate"
	"io.translab/tantor-agent/pkg/api"
)

type addedCleanupProcess struct {
	OperationID    string   `json:"operationId"`
	PlanDigest     string   `json:"planDigest"`
	HostID         string   `json:"hostId"`
	Role           string   `json:"role"`
	NodeID         int      `json:"nodeId"`
	Unit           string   `json:"unit"`
	ConfigPath     string   `json:"configPath"`
	KafkaClusterID string   `json:"kafkaClusterId"`
	DataPaths      []string `json:"dataPaths"`
	MetadataPath   string   `json:"metadataPath"`
	LogPath        string   `json:"logPath"`
	RuntimeHome    string   `json:"runtimeHome"`
}
type addedCleanupPlan struct {
	SchemaVersion string                `json:"schemaVersion"`
	ClusterID     string                `json:"clusterId"`
	HostID        string                `json:"hostId"`
	Processes     []addedCleanupProcess `json:"processes"`
}
type addedCleanupJournal struct {
	Digest     string            `json:"digest"`
	Validated  bool              `json:"validated"`
	Stopped    bool              `json:"stopped"`
	Complete   bool              `json:"complete"`
	Done       map[string]bool   `json:"done"`
	FileHashes map[string]string `json:"fileHashes"`
	Removing   map[string]bool   `json:"removing"`
}

func (d *Deployer) cleanAddedNodes(ctx context.Context, t *api.Task) (string, error) {
	return d.cleanAddedNodesAt(ctx, t, "/etc/tantor-kafka", "/etc/systemd/system")
}
func (d *Deployer) cleanAddedNodesAt(ctx context.Context, t *api.Task, configRoot, unitRoot string) (string, error) {
	raw := t.Parameters["add_node_cleanup"]
	var plan addedCleanupPlan
	if os.Geteuid() != 0 || len(raw) > 65536 || json.Unmarshal([]byte(raw), &plan) != nil || plan.SchemaVersion != "add-node-cleanup/v1" || !addedOperation.MatchString(plan.ClusterID) || plan.ClusterID != t.ClusterID && plan.ClusterID != t.Parameters["db_cluster_id"] || plan.HostID != d.cfg.Agent.HostID || len(plan.Processes) < 1 || len(plan.Processes) > 2 {
		return "", fmt.Errorf("invalid receipt-backed cleanup scope")
	}
	sum := sha256.Sum256([]byte(raw))
	digest := hex.EncodeToString(sum[:])
	journalRoot := filepath.Join(d.cfg.Paths.DataDir, "add-node-cleanup")
	if err := os.MkdirAll(journalRoot, 0700); err != nil {
		return "", err
	}
	actual, err := filepath.EvalSymlinks(journalRoot)
	if err != nil || actual != journalRoot {
		return "", fmt.Errorf("cleanup state directory must be canonical")
	}
	lock, err := os.OpenFile(filepath.Join(journalRoot, "host.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return "", err
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return "", fmt.Errorf("another owned cleanup is in progress")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	journalPath := filepath.Join(journalRoot, plan.ClusterID+".json")
	journal := addedCleanupJournal{Digest: digest, Done: map[string]bool{}, FileHashes: map[string]string{}, Removing: map[string]bool{}}
	if _, err = os.Lstat(journalPath); err == nil {
		body, e := readAddedOwned(journalPath, 65536)
		if e != nil {
			return "", e
		}
		if json.Unmarshal(body, &journal) != nil || journal.Digest != digest || journal.Done == nil || journal.FileHashes == nil || journal.Removing == nil {
			return "", fmt.Errorf("cleanup request changed after intent")
		}
	} else if !os.IsNotExist(err) {
		return "", err
	}
	save := func() error {
		body, err := json.Marshal(journal)
		if err != nil {
			return err
		}
		file, err := os.CreateTemp(journalRoot, ".cleanup-")
		if err != nil {
			return err
		}
		name := file.Name()
		defer os.Remove(name)
		if err = file.Chmod(0600); err == nil {
			_, err = file.Write(body)
		}
		if err == nil {
			err = file.Sync()
		}
		closeErr := file.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
		if err = os.Rename(name, journalPath); err != nil {
			return err
		}
		directory, err := os.Open(journalRoot)
		if err != nil {
			return err
		}
		defer directory.Close()
		return directory.Sync()
	}
	if journal.Complete {
		return "[PASS] Receipt-backed added-node cleanup was already completed.\n", nil
	}
	paths := []string{}
	units := map[string]bool{}
	files := map[string]bool{}
	for _, p := range plan.Processes {
		if p.HostID != plan.HostID || !addedOperation.MatchString(p.OperationID) || p.Unit != "tantor-kafka-"+p.Role+"-"+strconv.Itoa(p.NodeID)+".service" || units[p.Unit] || p.Role != "broker" && p.Role != "controller" || len(p.DataPaths) < 1 || len(p.DataPaths) > 16 {
			return "", fmt.Errorf("ambiguous cleanup process ownership")
		}
		units[p.Unit] = true
		configParent := filepath.Dir(p.ConfigPath)
		relative, e := filepath.Rel(configRoot, configParent)
		parts := strings.Split(relative, string(os.PathSeparator))
		if e != nil || len(parts) != 2 || (parts[0] != "add-broker" && parts[0] != "add-expansion") || parts[1] != p.OperationID {
			return "", fmt.Errorf("configuration escaped its operation namespace")
		}
		storage := append([]string{}, p.DataPaths...)
		if p.MetadataPath != "" && !containsCleanup(storage, p.MetadataPath) {
			storage = append(storage, p.MetadataPath)
		}
		for _, path := range append(storage, p.LogPath) {
			clean, e := taskvalidate.ApprovedPath(path)
			if e != nil || clean != path || cleanupOverlap(path, p.RuntimeHome) || cleanupOverlap(path, d.cfg.Paths.DataDir) {
				return "", fmt.Errorf("cleanup path overlaps immutable runtime or agent state")
			}
			for _, other := range paths {
				if cleanupOverlap(path, other) {
					return "", fmt.Errorf("cleanup paths overlap")
				}
			}
			paths = append(paths, path)
		}
		unitPath := filepath.Join(unitRoot, p.Unit)
		files[p.ConfigPath] = true
		files[unitPath] = true
		if !journal.Validated {
			task := &api.Task{Parameters: map[string]string{"config_path": p.ConfigPath, "role": p.Role, "node_id": strconv.Itoa(p.NodeID), "service_name": p.Unit}}
			if _, e := validateAddedConfigAt(task, d.cfg.Paths.DataDir, plan.HostID, configRoot, unitRoot); e != nil {
				return "", e
			}
			receiptPath := filepath.Join(d.cfg.Paths.DataDir, "quorum-expansion", p.OperationID, p.Role+".json")
			if parts[0] == "add-broker" {
				receiptPath = filepath.Join(d.cfg.Paths.DataDir, "broker-addition", p.OperationID, "receipt.json")
			}
			body, e := readAddedOwned(receiptPath, 32768)
			if e != nil {
				return "", e
			}
			var receipt struct {
				PlanDigest, KafkaClusterID string
				DirectoryIDs               map[string]string
			}
			if json.Unmarshal(body, &receipt) != nil || receipt.PlanDigest != p.PlanDigest || receipt.KafkaClusterID != p.KafkaClusterID {
				return "", fmt.Errorf("cleanup receipt digest or cluster identity changed")
			}
			configBytes, e := readAddedOwned(p.ConfigPath, 1048576)
			if e != nil {
				return "", e
			}
			props, e := cleanupProperties(string(configBytes))
			if e != nil {
				return "", e
			}
			if props["log.dirs"] != strings.Join(p.DataPaths, ",") || props["metadata.log.dir"] != p.MetadataPath && !(p.MetadataPath == "" && props["metadata.log.dir"] == "") {
				return "", fmt.Errorf("cleanup storage differs from the owned configuration")
			}
			unitBytes, e := readAddedOwned(unitPath, 131072)
			if e != nil {
				return "", e
			}
			if !strings.Contains(string(unitBytes), "\nEnvironment=LOG_DIR="+p.LogPath+"\n") {
				return "", fmt.Errorf("cleanup log path differs from the owned unit")
			}
			if _, e = os.Lstat(unitPath + ".d"); e == nil || !os.IsNotExist(e) {
				return "", fmt.Errorf("custom unit overrides require explicit cleanup reconciliation")
			}
			seen := map[string]bool{}
			for _, path := range storage {
				if e = cleanupTree(path); e != nil {
					return "", e
				}
				meta, e := os.ReadFile(filepath.Join(path, "meta.properties"))
				if e != nil {
					return "", e
				}
				values, e := cleanupProperties(string(meta))
				id := values["directory.id"]
				if e != nil || values["cluster.id"] != p.KafkaClusterID || values["node.id"] != strconv.Itoa(p.NodeID) || !regexpCleanupDirectory(id) || seen[id] || receipt.DirectoryIDs != nil && receipt.DirectoryIDs[path] != id {
					return "", fmt.Errorf("cleanup storage identity is foreign or incomplete")
				}
				seen[id] = true
			}
			if e = cleanupTree(p.LogPath); e != nil {
				return "", e
			}
			journal.FileHashes[p.ConfigPath] = cleanupHash(configBytes)
			journal.FileHashes[unitPath] = cleanupHash(unitBytes)
		}
	}
	if !journal.Validated {
		journal.Validated = true
		if err = save(); err != nil {
			return "", err
		}
	}
	for path := range files {
		if journal.Done[path] {
			continue
		}
		if _, e := os.Lstat(path); os.IsNotExist(e) && journal.Removing[path] && journal.Stopped {
			continue
		}
		body, e := readAddedOwned(path, 1048576)
		if e != nil || cleanupHash(body) != journal.FileHashes[path] {
			return "", fmt.Errorf("validated cleanup file changed; reservations must remain held")
		}
	}
	for unit := range units {
		if !journal.Done[filepath.Join(unitRoot, unit)] {
			if _, detail, e := d.exec.RunSudo(ctx, "systemctl", "stop", unit); e != nil {
				return "", fmt.Errorf("stop owned unit: %w (%s)", e, detail)
			}
			if _, detail, e := d.exec.RunSudo(ctx, "systemctl", "disable", unit); e != nil {
				return "", fmt.Errorf("disable owned unit: %w (%s)", e, detail)
			}
		}
		pid, _, e := d.exec.RunSudo(ctx, "systemctl", "show", unit, "--property=MainPID", "--value")
		if e != nil || strings.TrimSpace(pid) != "0" {
			return "", fmt.Errorf("owned unit has not fully stopped")
		}
	}
	journal.Stopped = true
	if err = save(); err != nil {
		return "", err
	}
	for _, path := range paths {
		if journal.Done[path] {
			continue
		}
		journal.Removing[path] = true
		if err = save(); err != nil {
			return "", err
		}
		if _, e := os.Lstat(path); e == nil {
			if e = cleanupTree(path); e != nil {
				return "", e
			}
			if e = os.RemoveAll(path); e != nil {
				return "", e
			}
		} else if !os.IsNotExist(e) {
			return "", e
		}
		journal.Done[path] = true
		if err = save(); err != nil {
			return "", err
		}
	}
	for path := range files {
		if journal.Done[path] {
			continue
		}
		journal.Removing[path] = true
		if err = save(); err != nil {
			return "", err
		}
		if err = os.Remove(path); err != nil && !os.IsNotExist(err) {
			return "", err
		}
		journal.Done[path] = true
		if err = save(); err != nil {
			return "", err
		}
	}
	if _, _, err = d.exec.RunSudo(ctx, "systemctl", "daemon-reload"); err != nil {
		return "", err
	}
	journal.Complete = true
	if err = save(); err != nil {
		return "", err
	}
	return "[PASS] Exact added-node units stopped and removed; verified role storage/logs purged. Shared Kafka parcels, operation receipts and retained config backups were preserved.\n", nil
}
func cleanupHash(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func cleanupOverlap(a, b string) bool {
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}
func containsCleanup(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
func regexpCleanupDirectory(value string) bool {
	if len(value) != 22 || value == "AAAAAAAAAAAAAAAAAAAAAA" {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}
func cleanupProperties(raw string) (map[string]string, error) {
	result := map[string]string{}
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "!") {
			continue
		}
		i := strings.IndexAny(line, "=:")
		if i < 1 || strings.HasSuffix(line, "\\") {
			return nil, fmt.Errorf("ambiguous cleanup properties")
		}
		key := strings.TrimSpace(line[:i])
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("duplicate cleanup property")
		}
		result[key] = strings.TrimSpace(line[i+1:])
	}
	return result, nil
}
func cleanupTree(path string) error {
	actual, err := filepath.EvalSymlinks(path)
	if err != nil || actual != path {
		return fmt.Errorf("cleanup tree must be canonical")
	}
	return filepath.WalkDir(path, func(_ string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.IsDir() && !entry.Type().IsRegular() {
			return fmt.Errorf("cleanup tree contains an unowned file type")
		}
		return nil
	})
}
