//go:build linux

package expansion

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestNativeConfigurationReplacementPreservesExtendedAttributes(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "controller.properties")
	if err := os.WriteFile(file, []byte("original"), 0640); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Setxattr(file, "user.tantor-review", []byte("verified-owner"), 0); err == syscall.ENOTSUP {
		t.Skip("filesystem has no extended attribute support")
	} else if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := durableReplace(file, "expanded", 0640, info); err != nil {
		t.Fatal(err)
	}
	value := make([]byte, 64)
	n, err := syscall.Getxattr(file, "user.tantor-review", value)
	if err != nil || string(value[:n]) != "verified-owner" {
		t.Fatal("configuration attributes lost", err)
	}
}
func TestFrozenParticipantAliasCannotRetargetEvenToIdenticalConfigBytes(t *testing.T) {
	root, err := os.MkdirTemp("/var/tmp", "tantor-frozen-alias-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	a, b := filepath.Join(root, "a"), filepath.Join(root, "b")
	for _, dir := range []string{a, b} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "controller.properties"), []byte("node.id=101\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	alias := filepath.Join(root, "current")
	if err := os.Symlink(a, alias); err != nil {
		t.Fatal(err)
	}
	logical := filepath.Join(alias, "controller.properties")
	m := Participant{ConfigPath: logical, ResolvedPaths: map[string]string{logical: filepath.Join(a, "controller.properties")}}
	if _, err := readParticipantFile(context.Background(), m, logical, 1024); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(b, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := readParticipantFile(context.Background(), m, logical, 1024); err == nil {
		t.Fatal("retargeted installation alias accepted")
	}
}
func TestCatalogParcelCacheUsesTheEstablishedParentDownloadLayout(t *testing.T) {
	root, err := os.MkdirTemp("/var/tmp", "tantor-parcel-cache-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	parcel := filepath.Join(root, "parcels")
	if err := os.Mkdir(parcel, 0700); err != nil {
		t.Fatal(err)
	}
	downloads := filepath.Join(root, ".downloads")
	if err := os.Mkdir(downloads, 0700); err != nil {
		t.Fatal(err)
	}
	raw := archiveFixture(t, nil)
	if err := os.WriteFile(filepath.Join(downloads, "parcel_kafka_4.3.0.tgz"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	m, err := loadManifest(context.Background(), Plan{ParcelDir: parcel, KafkaVersion: "4.3.0", ArtifactSHA256: Digest(string(raw))})
	if err != nil || len(m.Files) != 6 {
		t.Fatal("established parcel cache rejected", m, err)
	}
}

func TestNativeOwnedFilesRejectSymlinksAndConflictingStaging(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "original.properties")
	if err := writeExclusive(file, "node.id=1\n", 0600); err != nil {
		t.Fatal(err)
	}
	if err := writeExclusive(file, "node.id=1\n", 0600); err != nil {
		t.Fatal(err)
	}
	if err := writeExclusive(file, "node.id=2\n", 0600); err == nil {
		t.Fatal("backup overwritten")
	}
	link := filepath.Join(dir, "alias.properties")
	if err := os.Symlink(file, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readBounded(context.Background(), link, 1024); err == nil {
		t.Fatal("symlink followed")
	}
	if err := writeExclusive(link, "node.id=1\n", 0600); err == nil {
		t.Fatal("symlink accepted as owned file")
	}
	if err := durableReplace(link, "node.id=2\n", 0600, nil); err == nil {
		t.Fatal("symlink replaced")
	}
}
func TestNativeConfigReplacementPreservesModeAndRejectsChangedTarget(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "broker.properties")
	if err := os.WriteFile(file, []byte("original"), 0640); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := durableReplace(file, "expanded", info.Mode().Perm(), info); err != nil {
		t.Fatal(err)
	}
	current, err := os.Stat(file)
	if err != nil || current.Mode().Perm() != 0640 {
		t.Fatal(current, err)
	}
	raw, err := os.ReadFile(file)
	if err != nil || string(raw) != "expanded" {
		t.Fatal(string(raw), err)
	}
	if err := durableReplace(file, "stale", 0600, info); err == nil {
		t.Fatal("stale inode reused")
	}
}
func TestNativeRegularReadsRespectBoundsAndUnknownStorageNeverLooksEmpty(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "payload")
	if err := os.WriteFile(file, []byte("12345"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readBounded(context.Background(), file, 4); err == nil {
		t.Fatal("unbounded read accepted")
	}
	if err := emptyDirectory(dir); err == nil {
		t.Fatal("foreign storage accepted")
	}
	empty := filepath.Join(dir, "new", "nested")
	if err := emptyDirectory(empty); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(dir, filepath.Join(dir, "alias")); err != nil {
		t.Fatal(err)
	}
	if err := emptyDirectory(filepath.Join(dir, "alias", "new")); err == nil {
		t.Fatal("storage ancestor symlink followed")
	}
}
func TestNativeTimedOutCLIStopsTheEntireProcessGroup(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := runProcessGroup(ctx, "/bin/sh", "-c", "sleep 20 & echo $! > '"+pidFile+"'; wait")
	if err == nil || time.Since(started) > 5*time.Second {
		t.Fatal("process-group timeout failed", err)
	}
	raw, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid < 1 {
		t.Fatal("missing child PID", err)
	}
	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	split := strings.LastIndex(string(stat), ") ")
	if split < 0 || !strings.HasPrefix(string(stat)[split+2:], "Z ") {
		t.Fatal("timed-out child is still executing")
	}
}
