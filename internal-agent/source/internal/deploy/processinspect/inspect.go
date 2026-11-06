// Package processinspect observes an existing Kafka process. It never changes
// services, configuration, storage, quorum membership or filesystem ownership.
package processinspect

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/internal/taskvalidate"
)

type Request struct {
	SchemaVersion  string    `json:"schemaVersion"`
	InspectionID   string    `json:"inspectionId"`
	HostID         string    `json:"hostId"`
	ContextHash    string    `json:"contextHash"`
	KafkaClusterID string    `json:"kafkaClusterId"`
	KafkaVersion   string    `json:"kafkaVersion"`
	NodeID         int       `json:"nodeId"`
	Role           string    `json:"role"`
	ServiceName    string    `json:"serviceName"`
	ConfigPath     string    `json:"configPath"`
	StartedAt      time.Time `json:"startedAt"`
	ExpiresAt      time.Time `json:"expiresAt"`
}
type Metadata struct {
	Path        string `json:"path"`
	ClusterID   string `json:"clusterId"`
	NodeID      int    `json:"nodeId"`
	DirectoryID string `json:"directoryId"`
}
type Report struct {
	SchemaVersion     string     `json:"schemaVersion"`
	InspectionID      string     `json:"inspectionId"`
	HostID            string     `json:"hostId"`
	ContextHash       string     `json:"contextHash"`
	KafkaClusterID    string     `json:"kafkaClusterId"`
	KafkaVersion      string     `json:"kafkaVersion"`
	NodeID            int        `json:"nodeId"`
	Role              string     `json:"role"`
	ServiceName       string     `json:"serviceName"`
	ConfigPath        string     `json:"configPath"`
	ConfigSHA256      string     `json:"configSha256"`
	UnitSHA256        string     `json:"unitSha256"`
	PID               int        `json:"pid"`
	ProcessStartTicks string     `json:"processStartTicks"`
	ServiceActive     bool       `json:"serviceActive"`
	Metadata          []Metadata `json:"metadata"`
	IdentityVerified  bool       `json:"identityVerified"`
	VersionVerified   bool       `json:"versionVerified"`
	ExecutionReady    bool       `json:"executionReady"`
	ObservedAt        time.Time  `json:"observedAt"`
	Blockers          []string   `json:"blockers"`
}

var uuid = regexp.MustCompile(`^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$`)
var hash = regexp.MustCompile(`^[a-f0-9]{64}$`)
var kafkaID = regexp.MustCompile(`^[A-Za-z0-9_-]{22}$`)
var version = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
var unit = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_@.-]{0,180}\.service$`)

func Validate(raw, hostID string) (Request, error) {
	var r Request
	if len(raw) > 16384 || json.Unmarshal([]byte(raw), &r) != nil {
		return r, fmt.Errorf("invalid process inspection request")
	}
	now := time.Now()
	if r.SchemaVersion != "add-node-process/v1" || !uuid.MatchString(r.InspectionID) || r.HostID != hostID || hostID == "" || !hash.MatchString(r.ContextHash) || !kafkaID.MatchString(r.KafkaClusterID) || !version.MatchString(r.KafkaVersion) || r.NodeID < 0 || r.NodeID > 2147483647 {
		return r, fmt.Errorf("process inspection identity mismatch")
	}
	if r.StartedAt.IsZero() || r.StartedAt.After(now.Add(5*time.Second)) || !r.ExpiresAt.After(now) || !r.ExpiresAt.After(r.StartedAt) || r.ExpiresAt.Sub(r.StartedAt) > 5*time.Minute {
		return r, fmt.Errorf("process inspection expired or clock inconsistent")
	}
	if canonicalRole(r.Role) == "" || r.Role != canonicalRole(r.Role) || !unit.MatchString(r.ServiceName) || strings.Contains(r.ServiceName, "..") || !safeConfig(r.ConfigPath) {
		return r, fmt.Errorf("invalid reviewed service/configuration identity")
	}
	return r, nil
}
func canonicalRole(role string) string {
	switch strings.ReplaceAll(strings.TrimSpace(role), " ", "") {
	case "broker":
		return "broker"
	case "controller":
		return "controller"
	case "broker,controller", "controller,broker":
		return "broker,controller"
	}
	return ""
}
func safeConfig(p string) bool {
	if !canonicalPath(p) {
		return false
	}
	base := path.Base(p)
	if base != "broker.properties" && base != "controller.properties" && base != "server.properties" {
		return false
	}
	return approved(p) || strings.HasPrefix(p, "/etc/kafka/") || strings.HasPrefix(p, "/etc/tantor-kafka/") || strings.HasPrefix(p, "/etc/confluent/")
}
func canonicalPath(p string) bool {
	return strings.HasPrefix(p, "/") && path.Clean(p) == p && !strings.ContainsAny(p, " \t\r\n\x00\\") && !strings.HasSuffix(p, "/")
}
func approved(p string) bool {
	v, err := taskvalidate.ApprovedPath(p)
	return err == nil && v == p && canonicalPath(p)
}

// Run validates a frozen observation contract; only fixed systemctl show and
// bounded reads are used. No raw command, file body or JVM arguments are returned.
func Run(ctx context.Context, raw, hostID string, e executor.Executor) (Report, error) {
	r, err := Validate(raw, hostID)
	if err != nil {
		return Report{}, err
	}
	return inspect(ctx, r, e, readRegular), nil
}

type reader func(string, int64) ([]byte, error)

func inspect(ctx context.Context, r Request, e executor.Executor, read reader) Report {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	report := Report{SchemaVersion: r.SchemaVersion, InspectionID: r.InspectionID, HostID: r.HostID, ContextHash: r.ContextHash, KafkaClusterID: r.KafkaClusterID, KafkaVersion: r.KafkaVersion, NodeID: r.NodeID, Role: r.Role, ServiceName: r.ServiceName, ConfigPath: r.ConfigPath, Metadata: []Metadata{}, Blockers: []string{}}
	block := func(code string) {
		for _, b := range report.Blockers {
			if b == code {
				return
			}
		}
		report.Blockers = append(report.Blockers, code)
	}
	first, err := observeUnit(ctx, r.ServiceName, e)
	unitOK := err == nil && first["Id"] == r.ServiceName && first["LoadState"] == "loaded" && first["ActiveState"] == "active" && first["SubState"] == "running"
	pid, _ := strconv.Atoi(first["MainPID"])
	if pid < 0 || pid > 2147483647 {
		pid = 0
	}
	report.PID = pid
	unitOK = unitOK && pid > 0 && pid <= 2147483647 && first["ExecMainStartTimestampMonotonic"] != "" && first["ExecMainStartTimestampMonotonic"] != "0"
	fragment := first["FragmentPath"]
	if unitOK && canonicalPath(fragment) && (strings.HasPrefix(fragment, "/etc/systemd/system/") || strings.HasPrefix(fragment, "/usr/lib/systemd/system/") || strings.HasPrefix(fragment, "/lib/systemd/system/")) {
		content, err := read(fragment, 262144)
		unitOK = err == nil
		if unitOK {
			report.UnitSHA256 = digest(content)
		}
	} else {
		unitOK = false
	}
	report.ServiceActive = unitOK
	if !unitOK {
		block("SERVICE_NOT_OBSERVED")
	}

	configRaw, configErr := read(r.ConfigPath, 262144)
	properties, parseErr := parse(configRaw, map[string]bool{"node.id": true, "process.roles": true, "log.dirs": true, "metadata.log.dir": true})
	configOK := configErr == nil && parseErr == nil && properties["node.id"] == strconv.Itoa(r.NodeID) && canonicalRole(properties["process.roles"]) == r.Role
	if configErr == nil {
		report.ConfigSHA256 = digest(configRaw)
	}
	if !configOK {
		block("CONFIG_IDENTITY_MISMATCH")
	}

	storageOK := false
	metadataHashes := map[string]string{}
	if configOK {
		dirs := strings.Split(properties["log.dirs"], ",")
		if properties["metadata.log.dir"] != "" {
			dirs = append(dirs, properties["metadata.log.dir"])
		}
		seen := map[string]bool{}
		directoryIDs := map[string]bool{}
		storageOK = len(dirs) > 0 && len(dirs) <= 17
		for _, dir := range dirs {
			dir = strings.TrimSpace(dir)
			if seen[dir] {
				continue
			}
			seen[dir] = true
			if !approved(dir) {
				storageOK = false
				continue
			}
			content, err := read(path.Join(dir, "meta.properties"), 16384)
			m, err2 := parse(content, map[string]bool{"cluster.id": true, "node.id": true, "directory.id": true})
			if err != nil || err2 != nil || m["cluster.id"] != r.KafkaClusterID || m["node.id"] != strconv.Itoa(r.NodeID) || !kafkaID.MatchString(m["directory.id"]) || directoryIDs[m["directory.id"]] {
				storageOK = false
				continue
			}
			directoryIDs[m["directory.id"]] = true
			metadataHashes[path.Join(dir, "meta.properties")] = digest(content)
			report.Metadata = append(report.Metadata, Metadata{Path: dir, ClusterID: m["cluster.id"], NodeID: r.NodeID, DirectoryID: m["directory.id"]})
		}
		storageOK = storageOK && len(seen) == len(report.Metadata)
	}
	if !storageOK {
		block("STORAGE_IDENTITY_MISMATCH")
	}

	processOK := false
	cmdFingerprint := ""
	if unitOK {
		cmd, err := read(fmt.Sprintf("/proc/%d/cmdline", pid), 65536)
		stat, err2 := read(fmt.Sprintf("/proc/%d/stat", pid), 16384)
		args := strings.Split(string(cmd), "\x00")
		hasConfig, hasKafka := false, false
		for _, arg := range args {
			hasConfig = hasConfig || arg == r.ConfigPath
			hasKafka = hasKafka || arg == "kafka.Kafka"
		}
		split := strings.LastIndex(string(stat), ") ")
		if split >= 0 {
			fields := strings.Fields(string(stat[split+2:]))
			if len(fields) > 19 && regexp.MustCompile(`^[1-9][0-9]*$`).MatchString(fields[19]) {
				report.ProcessStartTicks = fields[19]
			}
		}
		processOK = err == nil && err2 == nil && len(args) > 0 && path.Base(args[0]) == "java" && hasConfig && hasKafka && report.ProcessStartTicks != ""
		if processOK {
			cmdFingerprint = digest(cmd)
		}
	}
	if !processOK {
		block("PROCESS_IDENTITY_MISMATCH")
	}
	second, secondErr := observeUnit(ctx, r.ServiceName, e)
	again, againErr := read(r.ConfigPath, 262144)
	if secondErr != nil || second["MainPID"] != first["MainPID"] || second["ExecMainStartTimestampMonotonic"] != first["ExecMainStartTimestampMonotonic"] || second["ActiveState"] != "active" || againErr != nil || digest(again) != report.ConfigSHA256 || ctx.Err() != nil || !r.ExpiresAt.After(time.Now()) {
		block("STALE_OBSERVATION")
	}
	for p, expected := range metadataHashes {
		b, err := read(p, 16384)
		if err != nil || digest(b) != expected {
			block("STALE_OBSERVATION")
		}
	}
	if unitOK {
		b, err := read(fragment, 262144)
		if err != nil || digest(b) != report.UnitSHA256 {
			block("STALE_OBSERVATION")
		}
	}
	if processOK {
		cmd, err := read(fmt.Sprintf("/proc/%d/cmdline", pid), 65536)
		stat, err2 := read(fmt.Sprintf("/proc/%d/stat", pid), 16384)
		split := strings.LastIndex(string(stat), ") ")
		start := ""
		if split >= 0 {
			fields := strings.Fields(string(stat[split+2:]))
			if len(fields) > 19 {
				start = fields[19]
			}
		}
		if err != nil || err2 != nil || digest(cmd) != cmdFingerprint || start != report.ProcessStartTicks {
			block("STALE_OBSERVATION")
		}
	}
	report.IdentityVerified = unitOK && configOK && storageOK && processOK && len(report.Blockers) == 0
	// This command does not prove artifact bytes, broker unfencing or voter state.
	block("VERSION_NOT_VERIFIED")
	report.ObservedAt = time.Now().UTC()
	return report
}
func observeUnit(ctx context.Context, name string, e executor.Executor) (map[string]string, error) {
	out, _, err := e.Run(ctx, "systemctl", "show", name, "--no-pager", "--property=Id,LoadState,ActiveState,SubState,MainPID,FragmentPath,ExecMainStartTimestampMonotonic")
	if err != nil || len(out) > 16384 {
		return map[string]string{}, fmt.Errorf("unit observation unavailable")
	}
	m := map[string]string{}
	for _, line := range strings.Split(out, "\n") {
		k, v, ok := strings.Cut(line, "=")
		if ok {
			if _, duplicate := m[k]; duplicate {
				return map[string]string{}, fmt.Errorf("ambiguous unit observation")
			}
			m[k] = strings.TrimSpace(v)
		}
	}
	return m, nil
}
func parse(content []byte, keys map[string]bool) (map[string]string, error) {
	m := map[string]string{}
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "!") {
			continue
		}
		i := strings.IndexAny(line, "=:")
		if i < 0 {
			return nil, fmt.Errorf("unsupported properties syntax")
		}
		key := strings.TrimSpace(line[:i])
		if strings.ContainsAny(key, "\\ \t") {
			return nil, fmt.Errorf("ambiguous property key")
		}
		if !keys[key] {
			continue
		}
		value := strings.TrimSpace(line[i+1:])
		if strings.Contains(value, "\\") {
			return nil, fmt.Errorf("escaped identity value unsupported")
		}
		if _, duplicate := m[key]; duplicate {
			return nil, fmt.Errorf("duplicate identity property")
		}
		m[key] = value
	}
	return m, nil
}
func digest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func readRegular(p string, limit int64) ([]byte, error) {
	for parent := p; parent != "/"; parent = path.Dir(parent) {
		info, err := os.Lstat(parent)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("unresolved or symlink observation path")
		}
	}
	info, err := os.Lstat(p)
	if err != nil || !info.Mode().IsRegular() || info.Size() > limit {
		return nil, fmt.Errorf("unreadable or oversized observation file")
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, fmt.Errorf("observation file unavailable")
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, fmt.Errorf("observation file changed during open")
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, fmt.Errorf("observation read exceeds bounds")
	}
	return data, nil
}
