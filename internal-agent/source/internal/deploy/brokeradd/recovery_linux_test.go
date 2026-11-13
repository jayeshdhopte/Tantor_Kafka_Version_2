//go:build linux

package brokeradd

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io.translab/tantor-agent/internal/deploy/layout"
	"io.translab/tantor-agent/internal/deploy/readiness"
	"io.translab/tantor-agent/internal/executor"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewNestedStorageIsUsableByNonRootRuntime(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}
	if _, err := user.Lookup("nobody"); err != nil {
		t.Skip("nobody unavailable")
	}
	root, err := os.MkdirTemp("/var/tmp", "tantor-broker-owner-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	if err := os.Chmod(root, 0755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "new-parent", "data")
	exec := executor.New(executor.Options{PrivilegeMode: "direct"})
	if err := runtimeDirectory(context.Background(), path, "nobody", exec); err != nil {
		t.Fatal(err)
	}
	if _, stderr, err := exec.Run(context.Background(), "runuser", "-u", "nobody", "--", "test", "-w", path); err != nil {
		t.Fatalf("runtime cannot write nested storage: %v %s", err, stderr)
	}
	info, err := os.Stat(root)
	if err != nil || info.Mode().Perm() != 0755 {
		t.Fatal("existing ancestor permissions changed")
	}
}

type fakeBrokerExecutor struct {
	plan       readiness.BrokerPlan
	formats    int
	partial    bool
	loseResult bool
	active     bool
}

func (e *fakeBrokerExecutor) Run(_ context.Context, command string, args ...string) (string, string, error) {
	if command == "systemctl" && len(args) > 0 && args[0] == "is-active" {
		if e.active {
			return "active\n", "", nil
		}
		return "inactive\n", "", fmt.Errorf("inactive")
	}
	return "", "", nil
}
func (e *fakeBrokerExecutor) RunSudo(_ context.Context, command string, args ...string) (string, string, error) {
	if command == "runuser" {
		joined := strings.Join(args, "\n")
		if !strings.Contains(joined, "LOG_DIR="+e.plan.LogPath+"\n") || !strings.Contains(joined, "KAFKA_LOG4J_OPTS=-Dlog4j2.configurationFile=") {
			return "", "", fmt.Errorf("formatter must use owned logging paths for non-root execution")
		}
		e.formats++
		for i, path := range storagePaths(e.plan) {
			if e.partial && i > 0 {
				break
			}
			id := fmt.Sprintf("%022d", i+1)
			raw := fmt.Sprintf("cluster.id=%s\nnode.id=%d\ndirectory.id=%s\n", e.plan.KafkaClusterID, e.plan.NodeID, id)
			if err := os.WriteFile(filepath.Join(path, "meta.properties"), []byte(raw), 0600); err != nil {
				return "", "", err
			}
		}
		if e.partial || e.loseResult {
			return "", "", fmt.Errorf("simulated callback loss or formatter failure")
		}
	}
	if command == "systemctl" && len(args) > 0 && args[0] == "enable" {
		e.active = true
	}
	return "", "", nil
}
func brokerTestFixture(t *testing.T, versions ...string) (readiness.BrokerPlan, map[string]string, string, string, string) {
	t.Helper()
	version := "4.1.2"
	if len(versions) > 0 {
		version = versions[0]
	}
	if os.Geteuid() != 0 {
		t.Skip("Linux root adapter test uses only an isolated /var/tmp test directory and fake commands")
	}
	root, err := os.MkdirTemp("/var/tmp", "tantor-broker-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if strings.HasPrefix(root, "/var/tmp/tantor-broker-test-") {
			os.RemoveAll(root)
		}
	})
	parcel := filepath.Join(root, "parcel")
	installed := filepath.Join(parcel, "kafka", version)
	os.MkdirAll(filepath.Join(installed, "bin"), 0755)
	script := []byte("fixture; never executed\n")
	os.WriteFile(filepath.Join(installed, "bin", "kafka-storage.sh"), script, 0755)
	os.MkdirAll(filepath.Join(parcel, ".downloads"), 0755)
	archive := filepath.Join(parcel, ".downloads", "parcel_kafka_"+version+".tgz")
	file, _ := os.Create(archive)
	gzipFile := gzip.NewWriter(file)
	tarFile := tar.NewWriter(gzipFile)
	tarFile.WriteHeader(&tar.Header{Name: "kafka/bin/kafka-storage.sh", Mode: 0755, Size: int64(len(script))})
	tarFile.Write(script)
	tarFile.Close()
	gzipFile.Close()
	file.Close()
	bytes, _ := os.ReadFile(archive)
	sum := sha256.Sum256(bytes)
	p := readiness.BrokerPlan{SchemaVersion: "add-broker/v1", OperationID: "12345678-1234-1234-1234-123456789abc", HostID: "fixture", ContextHash: strings.Repeat("a", 64), KafkaClusterID: "abcdefghijklmnopqrstuv", NodeID: 4, HostIP: "192.0.2.20", BrokerPort: 9092, KafkaVersion: version, RuntimeUser: "root", JavaHome: filepath.Join(root, "java"), ParcelDir: parcel, ArtifactChecksum: hex.EncodeToString(sum[:]), DataPaths: []string{filepath.Join(root, "data-one"), filepath.Join(root, "data-two")}, LogPath: filepath.Join(root, "logs"), InstallPath: filepath.Join(root, "install"), HeapXmx: "1g", QuorumMode: "DYNAMIC", ControllerEndpoints: []string{"192.0.2.10:9093"}}
	raw, _ := json.Marshal(p)
	props := p.Properties()
	params := map[string]string{"broker_plan": string(raw), "server_properties": props, "plan_digest": Digest(string(raw), props)}
	base := filepath.Join(root, "agent")
	config := filepath.Join(root, "etc-config")
	units := filepath.Join(root, "units")
	os.MkdirAll(units, 0755)
	return p, params, base, config, units
}
func TestNewBrokerUsesBrokerPropertiesAndKeepsFormatCheckpoint(t *testing.T) {
	p, params, base, config, units := brokerTestFixture(t, "4.3.0")
	p.SchemaVersion = "add-broker/v2"
	raw, _ := json.Marshal(p)
	params["broker_plan"] = string(raw)
	params["plan_digest"] = Digest(string(raw), params["server_properties"])
	e := &fakeBrokerExecutor{plan: p, loseResult: true}
	ctx := context.Background()
	if _, err := run(ctx, "PREPARE_ADD_BROKER", p.HostID, base, params, e, config, units); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(config, p.OperationID, "broker.properties")
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(config, p.OperationID, "server.properties")); !os.IsNotExist(err) {
		t.Fatal("new plan used the legacy filename")
	}
	unit, err := os.ReadFile(filepath.Join(units, "tantor-kafka-broker-4.service"))
	if err != nil || !strings.Contains(string(unit), path) {
		t.Fatal("service does not use broker.properties")
	}
	if _, err := run(ctx, "FORMAT_ADD_BROKER", p.HostID, base, params, e, config, units); err == nil {
		t.Fatal("callback loss not exercised")
	}
	if _, err := run(ctx, "FORMAT_ADD_BROKER", p.HostID, base, params, e, config, units); err != nil {
		t.Fatal(err)
	}
	if e.formats != 1 || storageState(p) != "MATCHING" {
		t.Fatal("resume changed broker storage identity")
	}
}
func TestFourThreeLostFormatResultPreservesIdentityAndDoesNotReformat(t *testing.T) {
	p, params, base, config, units := brokerTestFixture(t, "4.3.0")
	exec := &fakeBrokerExecutor{plan: p, loseResult: true}
	ctx := context.Background()
	if _, err := run(ctx, "PREPARE_ADD_BROKER", p.HostID, base, params, exec, config, units); err != nil {
		t.Fatal(err)
	}
	if _, err := run(ctx, "FORMAT_ADD_BROKER", p.HostID, base, params, exec, config, units); err == nil {
		t.Fatal("simulated formatter callback failure not propagated")
	}
	if _, err := run(ctx, "FORMAT_ADD_BROKER", p.HostID, base, params, exec, config, units); err != nil {
		t.Fatal(err)
	}
	if _, err := run(ctx, "START_ADD_BROKER", p.HostID, base, params, exec, config, units); err != nil {
		t.Fatal(err)
	}
	if exec.formats != 1 || p.KafkaVersion != "4.3.0" || storageState(p) != "MATCHING" {
		t.Fatal("4.3 identity/storage was not preserved during resume")
	}
}
func TestPartialFormatCannotReplayFormatter(t *testing.T) {
	p, params, base, config, units := brokerTestFixture(t)
	exec := &fakeBrokerExecutor{plan: p, partial: true}
	ctx := context.Background()
	if _, err := run(ctx, "PREPARE_ADD_BROKER", p.HostID, base, params, exec, config, units); err != nil {
		t.Fatal(err)
	}
	if _, err := run(ctx, "FORMAT_ADD_BROKER", p.HostID, base, params, exec, config, units); err == nil {
		t.Fatal("partial format succeeded")
	}
	if _, err := run(ctx, "FORMAT_ADD_BROKER", p.HostID, base, params, exec, config, units); err == nil {
		t.Fatal("unknown format replay accepted")
	}
	if exec.formats != 1 {
		t.Fatalf("formatter replayed %d times", exec.formats)
	}
}
func TestLostFormatResultReconcilesWithoutReformatting(t *testing.T) {
	p, params, base, config, units := brokerTestFixture(t)
	exec := &fakeBrokerExecutor{plan: p, loseResult: true}
	ctx := context.Background()
	if _, err := run(ctx, "PREPARE_ADD_BROKER", p.HostID, base, params, exec, config, units); err != nil {
		t.Fatal(err)
	}
	if _, err := run(ctx, "FORMAT_ADD_BROKER", p.HostID, base, params, exec, config, units); err == nil {
		t.Fatal("simulated callback loss not propagated")
	}
	if _, err := run(ctx, "FORMAT_ADD_BROKER", p.HostID, base, params, exec, config, units); err != nil {
		t.Fatal(err)
	}
	if exec.formats != 1 {
		t.Fatal("matching storage reformatted")
	}
	if _, err := run(ctx, "START_ADD_BROKER", p.HostID, base, params, exec, config, units); err != nil {
		t.Fatal(err)
	}
	if _, err := run(ctx, "START_ADD_BROKER", p.HostID, base, params, exec, config, units); err != nil {
		t.Fatal(err)
	}
	if exec.formats != 1 || !exec.active {
		t.Fatal("start replay reformatted storage or failed service")
	}
}

func TestStandardBrokerLayoutResumesWithoutReformatting(t *testing.T) {
	p, params, base, config, units := brokerTestFixture(t, "4.3.0")
	root := filepath.Dir(base)
	os.MkdirAll(filepath.Join(p.ParcelDir, "kafka", p.KafkaVersion, "libs"), 0755)
	p.SchemaVersion = "add-broker/v3"
	p.DeploymentSettings = &layout.Settings{InstallPath: p.InstallPath, ConfigDirectory: filepath.Join(p.InstallPath, "config"), JavaHome: p.JavaHome, RuntimeUser: p.RuntimeUser, BrokerDataPaths: p.DataPaths, BrokerLogPath: p.LogPath, BrokerHeapXmx: p.HeapXmx, ControllerDataPaths: []string{filepath.Join(root, "controller-data")}, ControllerMetadataPath: filepath.Join(root, "controller-metadata"), ControllerLogPath: filepath.Join(root, "controller-log"), ControllerHeapXmx: "512M"}
	raw, _ := json.Marshal(p)
	params["broker_plan"] = string(raw)
	params["plan_digest"] = Digest(string(raw), params["server_properties"])
	e := &fakeBrokerExecutor{plan: p, loseResult: true}
	ctx := context.Background()
	if _, err := run(ctx, "PREPARE_ADD_BROKER", p.HostID, base, params, e, config, units); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(p.InstallPath, "config", "broker.properties")
	unit, err := os.ReadFile(filepath.Join(units, "broker.service"))
	if err != nil || !strings.Contains(string(unit), path) || !strings.Contains(string(unit), p.InstallPath+"/bin/kafka-server-start.sh") {
		t.Fatal("nonstandard service", err)
	}
	if _, err := run(ctx, "FORMAT_ADD_BROKER", p.HostID, base, params, e, config, units); err == nil {
		t.Fatal("callback loss not exercised")
	}
	if _, err := run(ctx, "FORMAT_ADD_BROKER", p.HostID, base, params, e, config, units); err != nil {
		t.Fatal(err)
	}
	if e.formats != 1 || storageState(p) != "MATCHING" {
		t.Fatal("standard-layout resume repeated formatting")
	}
}
