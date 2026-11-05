package readiness

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type readOnlyExecutor struct{ commands []string }

func (e *readOnlyExecutor) Run(_ context.Context, cmd string, args ...string) (string, string, error) {
	e.commands = append(e.commands, cmd+" "+strings.Join(args, " "))
	return "observed", "", fmt.Errorf("unavailable measurement")
}
func (e *readOnlyExecutor) RunSudo(context.Context, string, ...string) (string, string, error) {
	panic("privileged mutation path used by readiness")
}
func TestMissingMeasurementsRemainBlockedAndNoMutationCommands(t *testing.T) {
	dir := t.TempDir()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := fmt.Sprint(listener.Addr().(*net.TCPAddr).Port)
	_ = listener.Close()
	exec := &readOnlyExecutor{}
	r := Request{HostIP: "127.0.0.1", BrokerPort: port, Ports: []string{port}, DataPaths: []string{filepath.Join(dir, "new-data")}, RuntimeUser: "kafka", MinAvailableMemoryMb: 4096, MinDiskGb: 20, MinInodes: 10000}
	report := Inspect(context.Background(), r, "candidate", exec)
	if len(report.Checks) != len(codes) {
		t.Fatal("incomplete contract")
	}
	for _, check := range report.Checks {
		if check.Code == "B06" && check.Status != "UNKNOWN" {
			t.Fatal("missing client-origin evidence passed")
		}
	}
	if _, err := os.Stat(r.DataPaths[0]); !os.IsNotExist(err) {
		t.Fatal("inspection created storage")
	}
	for _, cmd := range exec.commands {
		for _, bad := range []string{"mkdir ", "rm ", "chmod ", "systemctl stop", "systemctl start", "kafka-storage", "add-controller"} {
			if strings.Contains(cmd, bad) {
				t.Fatalf("mutation command: %s", cmd)
			}
		}
	}
}
func TestStorageRejectsAnyExistingData(t *testing.T) {
	dir := t.TempDir()
	if err := emptyStorage(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.properties"), []byte("cluster.id=foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	if emptyStorage(dir) == nil {
		t.Fatal("foreign storage accepted")
	}
	data, _ := os.ReadFile(filepath.Join(dir, "meta.properties"))
	if string(data) != "cluster.id=foreign" {
		t.Fatal("storage changed")
	}
}
func TestCatalogParcelCacheTakesPrecedenceOverLegacySameVersionArchive(t *testing.T) {
	root := t.TempDir()
	parcel := filepath.Join(root, "parcel")
	installed := filepath.Join(root, "installed")
	for _, dir := range []string{parcel, installed} {
		if err := os.MkdirAll(filepath.Join(dir, ".downloads"), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, ".downloads", "parcel_kafka_4.3.0.tgz"), []byte(dir), 0600); err != nil {
			t.Fatal(err)
		}
	}
	got := findArchive(Request{KafkaVersion: "4.3.0", InstallPath: installed, ParcelDir: parcel})
	if got != filepath.Join(parcel, ".downloads", "parcel_kafka_4.3.0.tgz") {
		t.Fatal("legacy cache shadowed the catalog-bound parcel")
	}
}
func TestNonceProbeRequiresExpectedIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	token := "12345678-1234-1234-1234-123456789012"
	session, err := listenProbe(ctx, "127.0.0.1:0", []PeerProbe{{HostID: "peer", SourceIP: "127.0.0.1", Roles: "BROKER,CLIENT", Token: token}})
	if err != nil {
		t.Fatal(err)
	}
	defer session.listener.Close()
	address := session.listener.Addr().(*net.TCPAddr)
	raw, _ := json.Marshal(ProbeRequest{HostID: "peer", TargetIP: "127.0.0.1", TargetPort: fmt.Sprint(address.Port), Token: token, ExpiresAt: time.Now().Add(time.Second)})
	if err := Probe(ctx, string(raw), "peer"); err != nil {
		t.Fatal(err)
	}
	if session.count("BROKER") != 1 || session.count("CLIENT") != 1 || session.count("CONTROLLER") != 0 {
		t.Fatal("incorrect probe evidence")
	}
	if err := Probe(ctx, string(raw), "other-host"); err == nil {
		t.Fatal("wrong source identity accepted")
	}
}
func TestValidationRejectsExpiredOrInjectedRequest(t *testing.T) {
	r := Request{SchemaVersion: "add-node/v1", HostID: "candidate", HostIP: "127.0.0.1", ContextHash: strings.Repeat("a", 64), ContextStartedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute), KafkaVersion: "4.1.2", RuntimeUser: "kafka;id", BrokerPort: "9092", LimitNofile: 1000, MinDiskGb: 1, MinInodes: 1, MinAvailableMemoryMb: 1}
	raw, _ := json.Marshal(r)
	if _, err := Validate(string(raw), "candidate"); err == nil {
		t.Fatal("injected runtime user accepted")
	}
	r.RuntimeUser = "kafka"
	r.ExpiresAt = time.Now().Add(-time.Second)
	raw, _ = json.Marshal(r)
	if _, err := Validate(string(raw), "candidate"); err == nil {
		t.Fatal("expired request accepted")
	}
}

func TestInstalledPayloadVerificationRejectsTamperingAndExtras(t *testing.T) {
	dir := t.TempDir()
	installed := filepath.Join(dir, "installed")
	if err := os.MkdirAll(filepath.Join(installed, "bin"), 0700); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(dir, "approved.tgz")
	file, err := os.Create(archive)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(file)
	writer := tar.NewWriter(gz)
	payload := []byte("approved script bytes")
	if err := writer.WriteHeader(&tar.Header{Name: "kafka/bin/tool.sh", Size: int64(len(payload)), Mode: 0700}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(payload); err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()
	_ = gz.Close()
	_ = file.Close()
	target := filepath.Join(installed, "bin", "tool.sh")
	if err := os.WriteFile(target, payload, 0700); err != nil {
		t.Fatal(err)
	}
	if err := verifyInstalledArchive(context.Background(), archive, installed); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(installed, "extra.jar"), []byte("unexpected"), 0600); err != nil {
		t.Fatal(err)
	}
	if verifyInstalledArchive(context.Background(), archive, installed) == nil {
		t.Fatal("extra executable payload accepted")
	}
	if err := os.Remove(filepath.Join(installed, "extra.jar")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("tampered"), 0700); err != nil {
		t.Fatal(err)
	}
	if verifyInstalledArchive(context.Background(), archive, installed) == nil {
		t.Fatal("tampered installed bytes accepted")
	}
}
