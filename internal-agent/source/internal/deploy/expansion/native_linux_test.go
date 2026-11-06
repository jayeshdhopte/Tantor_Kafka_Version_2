//go:build linux

package expansion

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

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
