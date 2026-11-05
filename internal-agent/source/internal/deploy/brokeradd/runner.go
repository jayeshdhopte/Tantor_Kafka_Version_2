// Package brokeradd owns only one new broker and never invokes legacy cleanup.
package brokeradd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"io.translab/tantor-agent/internal/client"
	"io.translab/tantor-agent/internal/deploy/kafka"
	"io.translab/tantor-agent/internal/deploy/readiness"
	"io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/pkg/logrotation"
)

type Receipt struct {
	SchemaVersion  string    `json:"schemaVersion"`
	OperationID    string    `json:"operationId"`
	HostID         string    `json:"hostId"`
	PlanDigest     string    `json:"planDigest"`
	Phase          string    `json:"phase"`
	KafkaClusterID string    `json:"kafkaClusterId"`
	NodeID         int       `json:"nodeId"`
	ServiceName    string    `json:"serviceName"`
	ConfigPath     string    `json:"configPath"`
	StorageState   string    `json:"storageState"`
	ServiceActive  bool      `json:"serviceActive"`
	ObservedAt     time.Time `json:"observedAt"`
}

func Digest(plan, properties string) string {
	sum := sha256.Sum256([]byte(plan + "\n" + properties))
	return hex.EncodeToString(sum[:])
}

// Run requires a root internal agent in the first adapter. The old sudo-mode
// deployment commands are unaffected. Rootless mutation is rejected explicitly.
func Run(ctx context.Context, command, hostID, base string, params map[string]string, exec executor.Executor, artifacts *client.APIClient) (string, error) {
	return run(ctx, command, hostID, base, params, exec, "/etc/tantor-kafka/add-broker", "/etc/systemd/system", artifacts.DownloadArtifactByID)
}
func run(ctx context.Context, command, hostID, base string, params map[string]string, exec executor.Executor, configRoot, unitRoot string, downloaders ...func(string, string) (string, error)) (string, error) {
	p, err := readiness.ValidateBrokerPlan(params["broker_plan"], hostID)
	if err != nil {
		return "", err
	}
	properties := params["server_properties"]
	digest := Digest(params["broker_plan"], properties)
	options := RuntimeOptions{LimitNofile: 100000}
	if raw := params["runtime_options"]; raw != "" {
		if json.Unmarshal([]byte(raw), &options) != nil || options.LimitNofile < 1024 || options.JmxPort < 0 || options.JmxPort > 65535 {
			return "", fmt.Errorf("invalid runtime options")
		}
		digest = DigestRuntime(params["broker_plan"], properties, raw)
	}
	if options.JmxPort > 0 && (!regexp.MustCompile(`^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$`).MatchString(options.JmxArtifactID) || !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(options.JmxChecksum) || options.JmxPort == p.BrokerPort) {
		return "", fmt.Errorf("invalid approved JMX exporter contract")
	}
	if digest != params["plan_digest"] || len(properties) > 262144 {
		return "", fmt.Errorf("broker payload digest mismatch")
	}
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		return "", fmt.Errorf("broker mutation adapter requires a Linux root internal agent")
	}
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	if err = validateProperties(p, properties); err != nil {
		return "", err
	}
	dir := filepath.Join(base, "broker-addition", p.OperationID)
	if err = noSymlinks(dir); err != nil {
		return "", err
	}
	if command != "INSPECT_ADD_BROKER" {
		if err = os.MkdirAll(dir, 0700); err != nil {
			return "", err
		}
	}
	configPath := filepath.Join(configRoot, p.OperationID, "server.properties")
	unitName := "tantor-kafka-broker-" + strconv.Itoa(p.NodeID)
	unitPath := filepath.Join(unitRoot, unitName+".service")
	if command == "INSPECT_ADD_BROKER" {
		if _, err = os.Stat(dir); os.IsNotExist(err) {
			r := Receipt{SchemaVersion: "add-broker/v1", OperationID: p.OperationID, HostID: hostID, PlanDigest: digest, Phase: "ABSENT", KafkaClusterID: p.KafkaClusterID, NodeID: p.NodeID, ServiceName: unitName, ConfigPath: configPath, StorageState: storageState(p), ObservedAt: time.Now().UTC()}
			encoded, _ := json.Marshal(r)
			return string(encoded), nil
		}
	}
	unlock, err := lock(filepath.Join(dir, "operation.lock"))
	if err != nil {
		return "", err
	}
	defer unlock()
	receiptPath := filepath.Join(dir, "receipt.json")
	receipt := Receipt{SchemaVersion: "add-broker/v1", OperationID: p.OperationID, HostID: hostID, PlanDigest: digest, Phase: "CLAIMED", KafkaClusterID: p.KafkaClusterID, NodeID: p.NodeID, ServiceName: unitName, ConfigPath: configPath}
	raw, readErr := os.ReadFile(receiptPath)
	if readErr == nil {
		if json.Unmarshal(raw, &receipt) != nil || receipt.PlanDigest != digest || receipt.OperationID != p.OperationID || receipt.HostID != hostID || receipt.ConfigPath != configPath || receipt.ServiceName != unitName || receipt.NodeID != p.NodeID || receipt.KafkaClusterID != p.KafkaClusterID {
			return "", fmt.Errorf("foreign or incompatible broker ownership receipt")
		}
	}
	if readErr != nil && !os.IsNotExist(readErr) {
		return "", readErr
	}
	if command == "INSPECT_ADD_BROKER" {
		if os.IsNotExist(readErr) {
			receipt.Phase = "ABSENT"
		} else if receipt.Phase != "CLAIMED" {
			if err = exactFile(configPath, properties); err != nil {
				return "", err
			}
			if err = exactFile(unitPath, unitContent(p, configPath, dir, options)); err != nil {
				return "", err
			}
			if err = verifyJmx(filepath.Dir(configPath), options); err != nil {
				return "", err
			}
			if err = exactFile(filepath.Join(filepath.Dir(configPath), "log4j2.xml"), logrotation.KafkaLog4j2(p.LogPath, unitName+".log")); err != nil {
				return "", err
			}
		}
		receipt.StorageState = storageState(p)
		out, _, activeErr := exec.Run(ctx, "systemctl", "is-active", unitName+".service")
		receipt.ServiceActive = activeErr == nil && strings.TrimSpace(out) == "active"
		receipt.ObservedAt = time.Now().UTC()
		encoded, _ := json.Marshal(receipt)
		return string(encoded), nil
	}
	if !map[string]bool{"PREPARE_ADD_BROKER": true, "FORMAT_ADD_BROKER": true, "START_ADD_BROKER": true}[command] {
		return "", fmt.Errorf("unknown broker mutation")
	}
	if err = readiness.ValidateBrokerPayload(ctx, p); err != nil {
		return "", err
	}
	if os.IsNotExist(readErr) {
		if command != "PREPARE_ADD_BROKER" {
			return "", fmt.Errorf("operation has no preparation receipt")
		}
		if _, err = os.Lstat(unitPath); !os.IsNotExist(err) {
			return "", fmt.Errorf("candidate unit already exists or is inaccessible")
		}
		for _, path := range storagePaths(p) {
			if err = empty(path); err != nil {
				return "", err
			}
		}
		if err = save(receiptPath, receipt); err != nil {
			return "", err
		}
	}
	unit := unitContent(p, configPath, dir, options)
	if receipt.Phase != "CLAIMED" {
		if err = exactFile(filepath.Join(filepath.Dir(configPath), "log4j2.xml"), logrotation.KafkaLog4j2(p.LogPath, unitName+".log")); err != nil {
			return "", err
		}
		if err = verifyJmx(filepath.Dir(configPath), options); err != nil {
			return "", err
		}
	}
	switch command {
	case "PREPARE_ADD_BROKER":
		if receipt.Phase != "CLAIMED" {
			if err = exactFile(configPath, properties); err != nil {
				return "", err
			}
			if err = exactFile(unitPath, unit); err != nil {
				return "", err
			}
			break
		}
		for _, path := range storagePaths(p) {
			if err = empty(path); err != nil {
				return "", err
			}
			if err = runtimeDirectory(ctx, path, p.RuntimeUser, exec); err != nil {
				return "", err
			}
		}
		if err = noSymlinks(p.LogPath); err != nil {
			return "", err
		}
		if err = runtimeDirectory(ctx, p.LogPath, p.RuntimeUser, exec); err != nil {
			return "", err
		}
		if err = noSymlinks(filepath.Dir(configPath)); err != nil {
			return "", err
		}
		if err = os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
			return "", err
		}
		if err = prepareJmx(filepath.Dir(configPath), options, downloaders); err != nil {
			return "", err
		}
		if err = writeOnce(filepath.Join(filepath.Dir(configPath), "log4j2.xml"), logrotation.KafkaLog4j2(p.LogPath, unitName+".log"), 0644); err != nil {
			return "", err
		}
		if err = writeOnce(configPath, properties, 0644); err != nil {
			return "", err
		}
		if err = writeOnce(unitPath, unit, 0644); err != nil {
			return "", err
		}
		receipt.Phase = "PREPARED"
	case "FORMAT_ADD_BROKER":
		if err = exactFile(configPath, properties); err != nil {
			return "", err
		}
		if err = exactFile(unitPath, unit); err != nil {
			return "", err
		}
		state := storageState(p)
		if receipt.Phase == "FORMAT_INTENT" {
			if state != "MATCHING" {
				return "", fmt.Errorf("format outcome uncertain; operator reconciliation required, formatter will not be replayed")
			}
			receipt.Phase = "FORMATTED"
			break
		}
		if receipt.Phase == "FORMATTED" || receipt.Phase == "START_INTENT" || receipt.Phase == "STARTED" {
			if state != "MATCHING" {
				return "", fmt.Errorf("formatted identity changed")
			}
			break
		}
		if receipt.Phase != "PREPARED" {
			return "", fmt.Errorf("broker is not prepared")
		}
		for _, path := range storagePaths(p) {
			if err = empty(path); err != nil {
				return "", err
			}
		}
		receipt.Phase = "FORMAT_INTENT"
		if err = save(receiptPath, receipt); err != nil {
			return "", err
		}
		args := []string{"-u", p.RuntimeUser, "--", "env", "JAVA_HOME=" + p.JavaHome, "KAFKA_HEAP_OPTS=-Xmx" + p.HeapXmx, "LOG_DIR=" + p.LogPath, "KAFKA_LOG4J_OPTS=-Dlog4j2.configurationFile=" + filepath.Join(filepath.Dir(configPath), "log4j2.xml") + " -Dkafka.logs.dir=" + p.LogPath, "bash", filepath.Join(p.ParcelDir, "kafka", p.KafkaVersion, "bin", "kafka-storage.sh"), "format", "--cluster-id", p.KafkaClusterID, "--config", configPath}
		if p.QuorumMode == "DYNAMIC" {
			args = append(args, "--no-initial-controllers")
		}
		if _, _, err = exec.RunSudo(ctx, "runuser", args...); err != nil {
			return "", fmt.Errorf("format failed; preserve receipt/storage and reconcile: %w", err)
		}
		if storageState(p) != "MATCHING" {
			return "", fmt.Errorf("formatted metadata does not match every expected storage directory")
		}
		receipt.Phase = "FORMATTED"
	case "START_ADD_BROKER":
		if receipt.Phase != "FORMATTED" && receipt.Phase != "FORMAT_INTENT" && receipt.Phase != "START_INTENT" && receipt.Phase != "STARTED" {
			return "", fmt.Errorf("broker not formatted")
		}
		if storageState(p) != "MATCHING" {
			return "", fmt.Errorf("storage identity mismatch")
		}
		if err = exactFile(configPath, properties); err != nil {
			return "", err
		}
		if err = exactFile(unitPath, unit); err != nil {
			return "", err
		}
		receipt.Phase = "START_INTENT"
		if err = save(receiptPath, receipt); err != nil {
			return "", err
		}
		if _, _, err = exec.RunSudo(ctx, "systemctl", "daemon-reload"); err != nil {
			return "", err
		}
		if _, _, err = exec.RunSudo(ctx, "systemctl", "enable", "--now", unitName+".service"); err != nil {
			return "", err
		}
		out, _, activeErr := exec.Run(ctx, "systemctl", "is-active", unitName+".service")
		if activeErr != nil || strings.TrimSpace(out) != "active" {
			return "", fmt.Errorf("candidate unit is not active")
		}
		receipt.Phase = "STARTED"
		receipt.ServiceActive = true
	}
	receipt.StorageState = storageState(p)
	receipt.ObservedAt = time.Now().UTC()
	if err = save(receiptPath, receipt); err != nil {
		return "", err
	}
	encoded, _ := json.Marshal(receipt)
	return string(encoded), nil
}

func storagePaths(p readiness.BrokerPlan) []string {
	paths := append([]string{}, p.DataPaths...)
	if p.MetadataPath != "" {
		found := false
		for _, v := range paths {
			found = found || v == p.MetadataPath
		}
		if !found {
			paths = append(paths, p.MetadataPath)
		}
	}
	return paths
}
func noSymlinks(path string) error {
	for v := filepath.Clean(path); v != filepath.Dir(v); v = filepath.Dir(v) {
		info, err := os.Lstat(v)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink path rejected: %s", v)
		}
	}
	return nil
}
func empty(path string) error {
	if err := noSymlinks(path); err != nil {
		return err
	}
	entries, err := os.ReadDir(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return fmt.Errorf("nonempty storage: %s", path)
	}
	return nil
}

// New ancestors must also be traversable by the Kafka runtime user. Existing
// ancestor ownership is never changed and no recursive chown is performed.
func runtimeDirectory(ctx context.Context, path, user string, exec executor.Executor) error {
	if err := noSymlinks(path); err != nil {
		return err
	}
	var missing []string
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("runtime ancestor is not a directory")
			}
			break
		}
		if !os.IsNotExist(err) {
			return err
		}
		missing = append(missing, current)
		if current == filepath.Dir(current) {
			return fmt.Errorf("runtime directory has no existing ancestor")
		}
	}
	for i := len(missing) - 1; i >= 0; i-- {
		if err := os.Mkdir(missing[i], 0750); err != nil {
			return err
		}
		if _, _, err := exec.RunSudo(ctx, "chown", user, "--", missing[i]); err != nil {
			return err
		}
	}
	if len(missing) == 0 {
		if _, _, err := exec.RunSudo(ctx, "chown", user, "--", path); err != nil {
			return err
		}
	}
	return nil
}
func storageState(p readiness.BrokerPlan) string {
	matches, missing := 0, 0
	identities := map[string]bool{}
	for _, path := range storagePaths(p) {
		if noSymlinks(path) != nil {
			return "FOREIGN"
		}
		meta := filepath.Join(path, "meta.properties")
		if noSymlinks(meta) != nil {
			return "FOREIGN"
		}
		raw, err := os.ReadFile(meta)
		if os.IsNotExist(err) {
			missing++
			continue
		}
		if err != nil {
			return "UNKNOWN"
		}
		props := map[string]string{}
		for _, line := range strings.Split(string(raw), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			kv := strings.SplitN(line, "=", 2)
			if len(kv) == 2 {
				props[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
			}
		}
		if props["cluster.id"] != p.KafkaClusterID || props["node.id"] != strconv.Itoa(p.NodeID) || !regexp.MustCompile(`^[A-Za-z0-9_-]{22}$`).MatchString(props["directory.id"]) || identities[props["directory.id"]] {
			return "FOREIGN"
		}
		identities[props["directory.id"]] = true
		matches++
	}
	if matches == len(storagePaths(p)) {
		return "MATCHING"
	}
	if missing == len(storagePaths(p)) {
		return "MISSING"
	}
	return "PARTIAL"
}
func exactFile(path, content string) error {
	if err := noSymlinks(path); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("nonregular owned file")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if string(raw) != content {
		return fmt.Errorf("owned file changed: %s", path)
	}
	return nil
}
func writeOnce(path, content string, mode os.FileMode) error {
	if err := noSymlinks(path); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if os.IsExist(err) {
		return exactFile(path, content)
	}
	if err != nil {
		return err
	}
	_, err = file.WriteString(content)
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	return closeErr
}
func save(path string, receipt Receipt) error {
	raw, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	_, err = file.Write(raw)
	if err == nil {
		err = file.Sync()
	}
	file.Close()
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func unitContent(p readiness.BrokerPlan, config, dir string, options ...RuntimeOptions) string {
	opts := RuntimeOptions{LimitNofile: 100000}
	if len(options) > 0 {
		opts = options[0]
	}
	jmx := ""
	if opts.JmxPort > 0 {
		jmx = fmt.Sprintf("Environment=KAFKA_OPTS=-javaagent:%s=%d:%s\n", filepath.Join(filepath.Dir(config), "jmx.jar"), opts.JmxPort, filepath.Join(filepath.Dir(config), "jmx.yml"))
	}
	return fmt.Sprintf("# Tantor Add Broker operation %s\n[Unit]\nDescription=Tantor Kafka broker %d\nAfter=network-online.target\n[Service]\nType=simple\nUser=%s\nEnvironment=JAVA_HOME=%s\nEnvironment=KAFKA_HEAP_OPTS=-Xmx%s\nEnvironment=LOG_DIR=%s\nEnvironment=\"KAFKA_LOG4J_OPTS=-Dlog4j2.configurationFile=%s/log4j2.xml -Dkafka.logs.dir=%s\"\n%sExecStart=%s/bin/kafka-server-start.sh %s\nRestart=no\nLimitNOFILE=%d\n[Install]\nWantedBy=multi-user.target\n", p.OperationID, p.NodeID, p.RuntimeUser, p.JavaHome, p.HeapXmx, p.LogPath, filepath.Dir(config), p.LogPath, jmx, filepath.Join(p.ParcelDir, "kafka", p.KafkaVersion), config, opts.LimitNofile)
}

type RuntimeOptions struct {
	JmxArtifactID string `json:"jmxArtifactId"`
	JmxChecksum   string `json:"jmxChecksum"`
	JmxPort       int    `json:"jmxPort"`
	LimitNofile   int    `json:"limitNofile"`
}

func DigestRuntime(plan, properties, options string) string {
	sum := sha256.Sum256([]byte(plan + "\n" + properties + "\n" + options))
	return hex.EncodeToString(sum[:])
}
func jarHash(path string) (string, error) {
	if err := noSymlinks(path); err != nil {
		return "", err
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() > 128*1024*1024 {
		return "", fmt.Errorf("invalid exporter artifact")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
func verifyJmx(dir string, options RuntimeOptions) error {
	if options.JmxPort == 0 {
		return nil
	}
	hash, err := jarHash(filepath.Join(dir, "jmx.jar"))
	if err != nil || hash != options.JmxChecksum {
		return fmt.Errorf("approved JMX exporter checksum mismatch")
	}
	return exactFile(filepath.Join(dir, "jmx.yml"), kafka.JmxConfigTemplate)
}
func prepareJmx(dir string, options RuntimeOptions, downloaders []func(string, string) (string, error)) error {
	if options.JmxPort == 0 {
		return nil
	}
	jar := filepath.Join(dir, "jmx.jar")
	if _, err := os.Lstat(jar); os.IsNotExist(err) {
		if len(downloaders) != 1 {
			return fmt.Errorf("approved exporter downloader is unavailable")
		}
		pending := jar + ".pending"
		if _, err = downloaders[0](options.JmxArtifactID, pending); err != nil {
			return err
		}
		hash, hashErr := jarHash(pending)
		if hashErr != nil || hash != options.JmxChecksum {
			return fmt.Errorf("downloaded JMX artifact checksum mismatch")
		}
		if err = os.Chmod(pending, 0644); err != nil {
			return err
		}
		if err = os.Rename(pending, jar); err != nil {
			return err
		}
	}
	if err := writeOnce(filepath.Join(dir, "jmx.yml"), kafka.JmxConfigTemplate, 0644); err != nil {
		return err
	}
	return verifyJmx(dir, options)
}
func validateProperties(p readiness.BrokerPlan, raw string) error {
	props := map[string]string{}
	for _, line := range strings.Split(raw, "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		kv := strings.SplitN(line, "=", 2)
		if len(kv) != 2 || strings.ContainsAny(kv[0], "\\ \t\r") || strings.ContainsAny(kv[1], "\r\x00") {
			return fmt.Errorf("invalid generated broker properties")
		}
		if _, ok := props[kv[0]]; ok {
			return fmt.Errorf("duplicate broker property")
		}
		props[kv[0]] = kv[1]
	}
	if props["process.roles"] != "broker" || props["node.id"] != strconv.Itoa(p.NodeID) || props["log.dirs"] != strings.Join(p.DataPaths, ",") || props["metadata.log.dir"] != p.MetadataPath {
		return fmt.Errorf("generated properties do not match broker identity/storage")
	}
	if props["broker.rack"] != p.BrokerRack {
		return fmt.Errorf("candidate rack differs from the frozen broker plan")
	}
	endpoint := net.JoinHostPort(p.HostIP, strconv.Itoa(p.BrokerPort))
	if props["listeners"] != "PLAINTEXT://"+endpoint || props["advertised.listeners"] != "PLAINTEXT://"+endpoint || props["inter.broker.listener.name"] != "PLAINTEXT" || props["controller.listener.names"] != "CONTROLLER" || props["listener.security.protocol.map"] != "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT" {
		return fmt.Errorf("generated listener/security configuration differs from approved broker adapter")
	}
	if p.QuorumMode == "STATIC" && props["controller.quorum.voters"] != p.QuorumVoters {
		return fmt.Errorf("static voter mismatch")
	}
	if p.QuorumMode == "DYNAMIC" && (props["controller.quorum.voters"] != "" || props["controller.quorum.bootstrap.servers"] != strings.Join(p.ControllerEndpoints, ",")) {
		return fmt.Errorf("dynamic discovery mismatch")
	}
	return nil
}
