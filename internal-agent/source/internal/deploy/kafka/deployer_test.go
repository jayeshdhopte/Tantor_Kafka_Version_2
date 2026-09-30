package kafka

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	agentexecutor "io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/pkg/api"
)

func TestGenerateZooKeeperBrokerConfig(t *testing.T) {
	installDir := t.TempDir()
	dataDir := t.TempDir()
	task := &api.Task{Parameters: map[string]string{
		"mode":                "zookeeper",
		"service_role":        "broker",
		"node_id":             "2",
		"config_file":         "server.properties",
		"zookeeper_connect":   "10.0.0.1:2181,10.0.0.2:2181",
		"replication_factor":  "2",
		"min_insync_replicas": "1",
		"num_partitions":      "3",
	}}

	d := &Deployer{}
	if err := d.generateConfigs(context.Background(), task, installDir, dataDir); err != nil {
		t.Fatalf("generateConfigs returned error: %v", err)
	}

	content, err := os.ReadFile(filepath.Join(installDir, "config", "server.properties"))
	if err != nil {
		t.Fatalf("read broker config: %v", err)
	}
	config := string(content)
	for _, expected := range []string{
		"broker.id=2",
		"zookeeper.connect=10.0.0.1:2181,10.0.0.2:2181",
		"default.replication.factor=2",
		"min.insync.replicas=1",
	} {
		if !strings.Contains(config, expected) {
			t.Errorf("broker config missing %q\n%s", expected, config)
		}
	}
	if strings.Contains(config, "process.roles=") {
		t.Errorf("ZooKeeper broker config contains KRaft process.roles\n%s", config)
	}
}

func TestGenerateZooKeeperServerConfigAndMyID(t *testing.T) {
	installDir := t.TempDir()
	dataDir := t.TempDir()
	task := &api.Task{Parameters: map[string]string{
		"mode":              "zookeeper",
		"service_role":      "zookeeper",
		"node_id":           "1002",
		"config_file":       "zookeeper.properties",
		"kafka_data_dir":    dataDir,
		"zookeeper_port":    "2181",
		"zookeeper_servers": "server.1001=10.0.0.1:2888:3888\nserver.1002=10.0.0.2:2888:3888",
	}}

	d := &Deployer{}
	if err := d.generateConfigs(context.Background(), task, installDir, dataDir); err != nil {
		t.Fatalf("generateConfigs returned error: %v", err)
	}

	configPath := filepath.Join(installDir, "config", "zookeeper.properties")
	content, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read ZooKeeper config: %v", err)
	}
	if !strings.Contains(string(content), "server.1002=10.0.0.2:2888:3888") {
		t.Fatalf("ZooKeeper ensemble members missing from config:\n%s", content)
	}

	myID, err := os.ReadFile(filepath.Join(dataDir, "zookeeper-data", "myid"))
	if err != nil {
		t.Fatalf("read myid: %v", err)
	}
	if strings.TrimSpace(string(myID)) != "1002" {
		t.Fatalf("unexpected myid %q", myID)
	}
}

func TestDeploymentModeDefaultsToKRaft(t *testing.T) {
	if got := deploymentModeForTask(&api.Task{Parameters: map[string]string{}}); got != "kraft" {
		t.Fatalf("expected kraft, got %q", got)
	}
}

func TestMergeCustomKafkaPropertiesRemovesConflictingQuorumKeys(t *testing.T) {
	base := "process.roles=controller\ncontroller.quorum.voters=1@old:9093\nnum.io.threads=8\n"
	merged := mergeCustomKafkaProperties(base, map[string]string{
		"process.roles":                       "controller",
		"controller.quorum.voters":            "",
		"controller.quorum.bootstrap.servers": "10.0.0.1:9093,10.0.0.2:9093",
	})

	if strings.Contains(merged, "controller.quorum.voters=") {
		t.Fatalf("dynamic config retained static voter property:\n%s", merged)
	}
	if !strings.Contains(merged, "controller.quorum.bootstrap.servers=10.0.0.1:9093,10.0.0.2:9093") {
		t.Fatalf("dynamic bootstrap servers missing:\n%s", merged)
	}
	if !strings.Contains(merged, "num.io.threads=8") {
		t.Fatalf("unmanaged custom property was removed:\n%s", merged)
	}
}

func TestUsesFlatKafkaConfigLayout(t *testing.T) {
	if usesFlatKafkaConfigLayout("3.9.2") {
		t.Fatal("Kafka 3.9 must retain the config/kraft layout")
	}
	if !usesFlatKafkaConfigLayout("4.0.0") {
		t.Fatal("Kafka 4 must use the flat config layout")
	}
}

func TestKafkaValidationBootstrapPrefersTargetHostIP(t *testing.T) {
	task := &api.Task{Parameters: map[string]string{
		"host_ip":       "192.168.3.194",
		"listener_port": "9092",
	}}

	if got := kafkaValidationBootstrapForTask(task, "9092"); got != "192.168.3.194:9092" {
		t.Fatalf("expected target host bootstrap, got %q", got)
	}
}

func TestKafkaValidationBootstrapUsesExplicitOverride(t *testing.T) {
	task := &api.Task{Parameters: map[string]string{
		"host_ip":                     "192.168.3.194",
		"validation_bootstrap_server": "192.168.3.208:19092",
	}}

	if got := kafkaValidationBootstrapForTask(task, "9092"); got != "192.168.3.208:19092" {
		t.Fatalf("expected explicit validation bootstrap, got %q", got)
	}
}

func TestGenerateKRaftConfigAcceptsControllerQuorumVotersAlias(t *testing.T) {
	installDir := t.TempDir()
	dataDir := t.TempDir()
	task := &api.Task{Parameters: map[string]string{
		"service_role":              "broker_controller",
		"node_id":                   "2",
		"host_ip":                   "192.168.3.194",
		"listener_port":             "9092",
		"controller_port":           "9093",
		"version":                   "4.1.0",
		"kraft_quorum_mode":         "static",
		"controller_quorum_voters":  "1@192.168.3.161:9093,2@192.168.3.194:9093",
		"cluster_uuid":              "cluster-identity-12345",
		"kafka_install_base_dir":    installDir,
		"kafka_data_dir":            dataDir,
		"replication_factor":        "2",
		"min_insync_replicas":       "1",
		"offsets_topic_replication": "2",
	}}

	d := &Deployer{}
	if err := d.generateConfigs(context.Background(), task, installDir, dataDir); err != nil {
		t.Fatalf("generateConfigs returned error: %v", err)
	}

	content, err := os.ReadFile(filepath.Join(installDir, "config", "server.properties"))
	if err != nil {
		t.Fatalf("read KRaft config: %v", err)
	}
	config := string(content)
	for _, expected := range []string{
		"node.id=2",
		"process.roles=broker,controller",
		"controller.quorum.voters=1@192.168.3.161:9093,2@192.168.3.194:9093",
		"advertised.listeners=PLAINTEXT://192.168.3.194:9092",
	} {
		if !strings.Contains(config, expected) {
			t.Errorf("KRaft config missing %q\n%s", expected, config)
		}
	}
}

func TestKafkaPortValidationTimeoutBounds(t *testing.T) {
	minTask := &api.Task{Parameters: map[string]string{"kafka_port_wait_seconds": "1"}}
	if got := kafkaPortValidationTimeout(minTask); got != 30*time.Second {
		t.Fatalf("expected minimum timeout 30s, got %s", got)
	}

	maxTask := &api.Task{Parameters: map[string]string{"kafka_port_wait_seconds": "9999"}}
	if got := kafkaPortValidationTimeout(maxTask); got != 600*time.Second {
		t.Fatalf("expected maximum timeout 600s, got %s", got)
	}
}

func TestValidateMetaPropertiesRequiresMatchingIdentity(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "meta.properties")
	if err := os.WriteFile(path, []byte("cluster.id=cluster-identity-12345\nnode.id=101\n"), 0600); err != nil {
		t.Fatalf("write meta.properties: %v", err)
	}
	dirs := []string{tmpDir}
	if err := validateMetaProperties(context.Background(), nil, dirs, "cluster-identity-12345", "101", true); err != nil {
		t.Fatalf("matching identity rejected: %v", err)
	}
	if err := validateMetaProperties(context.Background(), nil, dirs, "different-cluster", "101", true); err == nil {
		t.Fatal("cluster identity mismatch was accepted")
	}
	if err := validateMetaProperties(context.Background(), nil, dirs, "cluster-identity-12345", "102", true); err == nil {
		t.Fatal("node identity mismatch was accepted")
	}
}

func TestValidateMetaPropertiesAllowsDifferentNodeWhenOnlyClusterIsChecked(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "meta.properties")
	if err := os.WriteFile(path, []byte("cluster.id=cluster-identity-12345\nnode.id=101\n"), 0600); err != nil {
		t.Fatalf("write meta.properties: %v", err)
	}

	err := validateMetaProperties(context.Background(), nil, []string{tmpDir}, "cluster-identity-12345", "1", false)
	if err != nil {
		t.Fatalf("same-cluster metadata from another role should not fail node validation: %v", err)
	}
}

func TestKafkaArtifactWorkDirRejectsPathOutsideApprovedRoots(t *testing.T) {
	base := filepath.Join(t.TempDir(), "agent-artifacts")
	task := &api.Task{Parameters: map[string]string{
		"artifact_load_dir": "/srv/tantor-agent",
	}}
	if _, err := kafkaArtifactWorkDir(task, base, base); err == nil {
		t.Fatal("expected an explicit error for an unapproved UI path")
	}
}

func TestKafkaArtifactWorkDirAllowsSubdirectoryInsideApprovedRoot(t *testing.T) {
	base := filepath.Join(t.TempDir(), "agent-artifacts")
	child := filepath.Join(base, "kafka")
	task := &api.Task{Parameters: map[string]string{
		"artifact_load_dir": child,
	}}
	got, err := kafkaArtifactWorkDir(task, base, base)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != filepath.Clean(child) {
		t.Fatalf("expected safe child directory %q, got %q", child, got)
	}
}

type metadataPermissionExecutor struct {
	files map[string]bool
	dirs  map[string]bool
}

func (e *metadataPermissionExecutor) Run(context.Context, string, ...string) (string, string, error) {
	return "", "", nil
}

func (e *metadataPermissionExecutor) RunSudo(_ context.Context, cmd string, args ...string) (string, string, error) {
	if cmd != "test" || len(args) != 2 {
		return "", "", nil
	}
	switch args[0] {
	case "-f":
		if e.files[args[1]] {
			return "", "", nil
		}
		return "", "", os.ErrNotExist
	case "-d":
		if e.dirs[args[1]] {
			return "", "", nil
		}
		return "", "", os.ErrNotExist
	default:
		return "", "", os.ErrInvalid
	}
}

func TestPrivilegedFileExistsUsesExecutorForRestrictedMetadata(t *testing.T) {
	metadataDir := "/data/kafka/broker-metadata"
	metadataFile := filepath.Join(metadataDir, "meta.properties")
	d := &Deployer{exec: &metadataPermissionExecutor{
		files: map[string]bool{metadataFile: true},
		dirs:  map[string]bool{metadataDir: true},
	}}

	exists, err := d.privilegedFileExists(context.Background(), metadataFile)
	if err != nil {
		t.Fatalf("privileged metadata inspection failed: %v", err)
	}
	if !exists {
		t.Fatal("expected privileged metadata inspection to find meta.properties")
	}
}

func TestPrivilegedFileExistsTreatsMissingFileAsFreshDeployment(t *testing.T) {
	metadataDir := "/data/kafka/broker-metadata"
	metadataFile := filepath.Join(metadataDir, "meta.properties")
	d := &Deployer{exec: &metadataPermissionExecutor{
		files: map[string]bool{},
		dirs:  map[string]bool{metadataDir: true},
	}}

	exists, err := d.privilegedFileExists(context.Background(), metadataFile)
	if err != nil {
		t.Fatalf("missing metadata should not be an inspection error: %v", err)
	}
	if exists {
		t.Fatal("expected missing meta.properties to trigger fresh deployment")
	}
}

type permissionNormalizationExecutor struct {
	commands []string
}

func (e *permissionNormalizationExecutor) Run(context.Context, string, ...string) (string, string, error) {
	return "", "", nil
}

func (e *permissionNormalizationExecutor) RunSudo(_ context.Context, cmd string, args ...string) (string, string, error) {
	e.commands = append(e.commands, cmd+" "+strings.Join(args, " "))
	return "", "", nil
}

func TestNormalizeKafkaTreePermissionsRepairsUploadedArchiveModes(t *testing.T) {
	exec := &permissionNormalizationExecutor{}
	d := &Deployer{exec: exec}
	if err := d.normalizeKafkaTreePermissions(context.Background(), "/opt/kafka_2.13-4.0.2"); err != nil {
		t.Fatalf("normalizeKafkaTreePermissions returned error: %v", err)
	}
	joined := strings.Join(exec.commands, "\n")
	if !strings.Contains(joined, "chmod -R a+rX") {
		t.Fatalf("permission normalization did not restore recursive read/traverse permissions:\n%s", joined)
	}
	if !strings.Contains(joined, "find") || !strings.Contains(joined, "chmod a+rx") {
		t.Fatalf("permission normalization did not restore Kafka shell launcher execute bits:\n%s", joined)
	}
	if !strings.Contains(joined, "kafka-storage.sh") {
		t.Fatalf("permission normalization did not validate kafka-storage.sh readability:\n%s", joined)
	}
}

func TestNormalizeKafkaTreePermissionsRejectsUnsafeRoot(t *testing.T) {
	d := &Deployer{exec: &permissionNormalizationExecutor{}}
	if err := d.normalizeKafkaTreePermissions(context.Background(), "/"); err == nil {
		t.Fatal("expected unsafe root directory to be rejected")
	}
}

func TestNormalizeKafkaTreePermissionsActuallyRepairsModeBits(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX executable mode bits are not represented on Windows")
	}
	installDir := t.TempDir()
	binDir := filepath.Join(installDir, "bin")
	if err := os.MkdirAll(binDir, 0o700); err != nil {
		t.Fatalf("mkdir bin: %v", err)
	}
	for _, name := range []string{"kafka-server-start.sh", "kafka-storage.sh"} {
		path := filepath.Join(binDir, name)
		if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	d := &Deployer{exec: agentexecutor.New(agentexecutor.Options{PrivilegeMode: "direct"})}
	if err := d.normalizeKafkaTreePermissionsAt(context.Background(), installDir); err != nil {
		t.Fatalf("normalizeKafkaTreePermissions returned error: %v", err)
	}

	for _, name := range []string{"kafka-server-start.sh", "kafka-storage.sh"} {
		info, err := os.Stat(filepath.Join(binDir, name))
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		if info.Mode().Perm()&0o111 == 0 {
			t.Fatalf("%s is still non-executable after normalization: mode=%#o", name, info.Mode().Perm())
		}
	}
}
