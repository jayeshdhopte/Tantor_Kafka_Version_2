package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io.translab/tantor-agent/pkg/javaupgrade"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func fakeJDK(t *testing.T) []byte {
	t.Helper()
	var out bytes.Buffer
	gz := gzip.NewWriter(&out)
	tw := tar.NewWriter(gz)
	arch := map[string]string{"amd64": "x86_64", "arm64": "aarch64"}[runtime.GOARCH]
	release := fmt.Sprintf("JAVA_VERSION=\"21.0.13\"\nOS_NAME=\"Linux\"\nOS_ARCH=\"%s\"\n", arch)
	for _, entry := range []struct {
		name, body string
		mode       int64
	}{{"jdk/release", release, 0644}, {"jdk/bin/java", "test-binary", 0755}} {
		if err := tw.WriteHeader(&tar.Header{Name: entry.name, Size: int64(len(entry.body)), Mode: entry.mode, Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(tw, entry.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}
func javaFixture(t *testing.T) (RuntimeConfig, DiscoveredCluster, managedTask) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "broker.properties")
	if err := os.WriteFile(path, []byte("node.id=7\ncluster.id=cluster-test\nlisteners=PLAINTEXT://:9092\n"), 0600); err != nil {
		t.Fatal(err)
	}
	record := DiscoveredCluster{KafkaMode: "kraft", NodeID: 7, KafkaClusterID: "cluster-test", SystemdService: "custom-broker.service", PropsFile: path, ProcessRoles: "broker", InstallPath: "/opt/kafka"}
	task := managedTask{ID: "00000000-0000-4000-8000-000000000001", Command: "STAGE_JAVA", Parameters: map[string]string{"node_id": "7", "kafka_cluster_id": "cluster-test", "config_path": path, "service_name": "custom-broker.service", "artifact_id": "00000000-0000-4000-8000-000000000002", "target_version": "21.0.13", "checksum": strings.Repeat("a", 64), "parcel_dir": filepath.Join(dir, "parcels")}}
	return RuntimeConfig{EnableTasks: true, AuthType: "bearer", AllowConfigWrite: true, AllowServiceRestart: true, AllowedServices: []string{"custom-broker.service"}}, record, task
}
func TestJavaListenerUsesLocalRoleAndRejectsSecureOnly(t *testing.T) {
	props := map[string]string{"listeners": "BROKER://:9092,CONTROLLER://:9093", "controller.listener.names": "CONTROLLER", "listener.security.protocol.map": "BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT"}
	for role, want := range map[string]string{"broker": "127.0.0.1:9092", "broker_controller": "127.0.0.1:9092", "controller": "127.0.0.1:9093"} {
		got, err := javaHealthEndpoint(props, role)
		if err != nil || got != want {
			t.Fatalf("%s: %s %v", role, got, err)
		}
	}
	props["listener.security.protocol.map"] = "BROKER:SASL_SSL,CONTROLLER:SSL"
	if _, err := javaHealthEndpoint(props, "broker"); err == nil {
		t.Fatal("secure listener accepted by plaintext probe")
	}
}
func TestExternalJavaRequiresExplicitCapabilities(t *testing.T) {
	for _, cfg := range []RuntimeConfig{{}, {EnableTasks: true, AuthType: "bearer"}, {EnableTasks: true, AuthType: "none", AllowConfigWrite: true}} {
		if validateJavaMode(cfg) == nil {
			t.Fatal("insufficient capabilities accepted")
		}
	}
}
func TestExternalJavaRefusesMultipleKafkaProcesses(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root-only capability")
	}
	cfg, r, task := javaFixture(t)
	other := r
	other.PropsFile = filepath.Join(filepath.Dir(r.PropsFile), "other.properties")
	if _, err := javaTargetParameters(cfg, r, []DiscoveredCluster{r, other}, task); err == nil {
		t.Fatal("shared VM accepted")
	}
}
func TestExternalJavaStagesVerifiedArchiveWithoutUpgrading(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root-only capability")
	}
	cfg, r, task := javaFixture(t)
	if !strings.HasPrefix(task.Parameters["parcel_dir"], "/var/") {
		t.Skip("run with TMPDIR=/var/tmp to test approved staging root")
	}
	body := fakeJDK(t)
	task.Parameters["checksum"] = fmt.Sprintf("%x", sha256.Sum256(body))
	called := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		called++
		if req.URL.Path != "/api/v1/artifacts/"+task.Parameters["artifact_id"]+"/download" {
			t.Error("unexpected artifact URL")
		}
		w.Write(body)
	}))
	defer server.Close()
	client := &APIClient{baseURL: server.URL, http: server.Client(), logger: slog.Default()}
	result := executeExternalJavaStage(context.Background(), client, cfg, []DiscoveredCluster{r}, task)
	if result.Status != "SUCCESS" {
		t.Fatal(result.Message)
	}
	archive, _ := javaupgrade.ArchivePath(task.Parameters["parcel_dir"], task.Parameters["artifact_id"])
	data, err := os.ReadFile(archive)
	if err != nil || !bytes.Equal(data, body) || called != 1 {
		t.Fatal("verified staged archive missing")
	}
	task.Parameters["checksum"] = strings.Repeat("0", 64)
	result = executeExternalJavaStage(context.Background(), client, cfg, []DiscoveredCluster{r}, task)
	if result.Status != "FAILED" || !strings.Contains(result.Message, "checksum") {
		t.Fatalf("bad checksum accepted: %+v", result)
	}
	data, _ = os.ReadFile(archive)
	if !bytes.Equal(data, body) {
		t.Fatal("failed distribution damaged previous staged archive")
	}
}
