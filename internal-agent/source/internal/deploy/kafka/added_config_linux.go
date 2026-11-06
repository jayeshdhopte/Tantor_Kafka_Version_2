//go:build linux

package kafka

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	"io.translab/tantor-agent/pkg/api"
)

var addedOperation = regexp.MustCompile(`^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$`)

// Existing installation rules remain unchanged. Only an agent-owned completed
// addition receipt can authorize configuration outside the installation tree.
func validateAddedConfig(task *api.Task, dataDir, hostID string) (string, error) {
	return validateAddedConfigAt(task, dataDir, hostID, "/etc/tantor-kafka", "/etc/systemd/system")
}

func validateAddedConfigAt(task *api.Task, dataDir, hostID, configRoot, unitRoot string) (string, error) {
	reject := func() (string, error) {
		return "", fmt.Errorf("added-node configuration ownership does not match this task")
	}
	requested := strings.TrimSpace(task.Parameters["config_path"])
	relative, err := filepath.Rel(configRoot, requested)
	if err != nil || requested != filepath.Clean(requested) || hostID == "" || !filepath.IsAbs(dataDir) {
		return reject()
	}
	parts := strings.Split(relative, string(os.PathSeparator))
	if len(parts) != 3 || !addedOperation.MatchString(parts[1]) {
		return reject()
	}
	family, op, file := parts[0], parts[1], parts[2]
	role := task.Parameters["role"]
	if role != "broker" && role != "controller" {
		return reject()
	}
	var receiptPath, schema, header string
	switch family {
	case "add-broker":
		if role != "broker" || file != "broker.properties" && file != "server.properties" {
			return reject()
		}
		receiptPath = filepath.Join(dataDir, "broker-addition", op, "receipt.json")
		schema = "add-broker/v1"
		header = "# Tantor Add Broker operation " + op
	case "add-expansion":
		if file != role+".properties" {
			return reject()
		}
		receiptPath = filepath.Join(dataDir, "quorum-expansion", op, role+".json")
		schema = "add-quorum-candidate/v1"
		header = "# Tantor quorum expansion " + op
	default:
		return reject()
	}
	raw, err := readAddedOwned(receiptPath, 32768)
	if err != nil {
		return "", err
	}
	var receipt struct {
		SchemaVersion, OperationID, HostID, PlanDigest, Phase, KafkaClusterID, ServiceName, Unit, Role, ConfigPath string
		NodeID                                                                                                     int
	}
	if json.Unmarshal(raw, &receipt) != nil || receipt.SchemaVersion != schema || receipt.OperationID != op || receipt.HostID != hostID || receipt.ConfigPath != requested || !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(receipt.PlanDigest) || !regexp.MustCompile(`^[A-Za-z0-9_-]{22}$`).MatchString(receipt.KafkaClusterID) {
		return reject()
	}
	if receipt.Phase != "STARTED" && !(family == "add-expansion" && (receipt.Phase == "ADMISSION_INTENT" || receipt.Phase == "ADMISSION_SENT")) {
		return reject()
	}
	if family == "add-expansion" && receipt.Role != role {
		return reject()
	}
	id, err := strconv.Atoi(task.Parameters["node_id"])
	if err != nil || id < 0 || id != receipt.NodeID {
		return reject()
	}
	unit := "tantor-kafka-" + role + "-" + strconv.Itoa(id) + ".service"
	recorded := receipt.Unit
	if family == "add-broker" {
		recorded = receipt.ServiceName + ".service"
	}
	if recorded != unit || strings.TrimSuffix(task.Parameters["service_name"], ".service") != strings.TrimSuffix(unit, ".service") {
		return reject()
	}
	unitBytes, err := readAddedOwned(filepath.Join(unitRoot, unit), 131072)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(string(unitBytes), header+"\n") {
		return reject()
	}
	starts := 0
	for _, line := range strings.Split(string(unitBytes), "\n") {
		if strings.HasPrefix(line, "ExecStart=") {
			starts++
			fields := strings.Fields(line)
			if len(fields) != 2 || fields[1] != requested || !strings.HasSuffix(fields[0], "/bin/kafka-server-start.sh") {
				return reject()
			}
		}
	}
	if starts != 1 {
		return reject()
	}
	configBytes, err := readAddedOwned(requested, 1048576)
	if err != nil {
		return "", err
	}
	values := map[string]string{}
	for _, line := range strings.Split(string(configBytes), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "!") {
			continue
		}
		if i := strings.IndexAny(line, "=:"); i >= 0 {
			key := strings.TrimSpace(line[:i])
			if key == "node.id" || key == "process.roles" {
				if _, exists := values[key]; exists {
					return reject()
				}
				values[key] = strings.TrimSpace(line[i+1:])
			}
		}
	}
	if values["node.id"] != strconv.Itoa(id) || values["process.roles"] != role {
		return reject()
	}
	return requested, nil
}

func readAddedOwned(path string, limit int64) ([]byte, error) {
	actual, err := filepath.EvalSymlinks(path)
	if err != nil || actual != path {
		return nil, fmt.Errorf("added-node file must have a canonical, non-symlink path")
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 || stat.Nlink != 1 || !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 || info.Size() > limit {
		return nil, fmt.Errorf("added-node file ownership or size is unsafe")
	}
	raw, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(raw)) > limit {
		return nil, fmt.Errorf("cannot read bounded added-node file")
	}
	return raw, nil
}
