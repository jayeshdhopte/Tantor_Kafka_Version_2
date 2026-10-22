package logrotation

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDailyRotationCompressesOldDayAndKeepsCurrentLog(t *testing.T) {
	dir := t.TempDir()
	w, err := New(dir, "agent.log")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	now := time.Now()
	w.now = func() time.Time { return now }
	if _, err = w.Write([]byte("previous day\n")); err != nil {
		t.Fatal(err)
	}
	now = now.AddDate(0, 0, 1)
	if _, err = w.Write([]byte("current day\n")); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "archive", "*.gz"))
	if len(files) != 1 {
		t.Fatal(files)
	}
	f, _ := os.Open(files[0])
	defer f.Close()
	zip, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer zip.Close()
	data, _ := io.ReadAll(zip)
	if string(data) != "previous day\n" {
		t.Fatal(string(data))
	}
	active, _ := os.ReadFile(filepath.Join(dir, "agent.log"))
	if string(active) != "current day\n" {
		t.Fatal(string(active))
	}
}
func TestSevenDayPruningOnlyRemovesManagedArchives(t *testing.T) {
	dir := t.TempDir()
	archive := filepath.Join(dir, "archive")
	os.Mkdir(archive, 0750)
	now := time.Now()
	old := filepath.Join(archive, "agent.log.2026-01-01.123.gz")
	fresh := filepath.Join(archive, "agent.log.2026-01-02.456.gz")
	foreign := filepath.Join(archive, "topic-data.log.gz")
	for _, p := range []string{old, fresh, foreign} {
		if err := os.WriteFile(p, []byte("sentinel"), 0640); err != nil {
			t.Fatal(err)
		}
	}
	past := now.Add(-8 * 24 * time.Hour)
	os.Chtimes(old, past, past)
	os.Chtimes(foreign, past, past)
	if err := Prune(dir, "agent.log", now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("old archive retained")
	}
	for _, p := range []string{fresh, foreign} {
		if _, err := os.Stat(p); err != nil {
			t.Fatal("unrelated/recent file removed", p)
		}
	}
}
func TestRotationRunsWithoutNewWritesAndRestartsArchivePreviousDay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.log")
	os.WriteFile(path, []byte("before restart"), 0640)
	old := time.Now().AddDate(0, 0, -1)
	os.Chtimes(path, old, old)
	w, err := New(dir, "agent.log")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	files, _ := filepath.Glob(filepath.Join(dir, "archive", "*.gz"))
	if len(files) != 1 {
		t.Fatal(files)
	}
}
func TestRetentionCommandDoesNotRequireBackendConfiguration(t *testing.T) {
	handled, err := PruneCommand([]string{"--prune-logs", "--log-dir", t.TempDir()}, "/unused", "agent.log")
	if !handled || err != nil {
		t.Fatal(handled, err)
	}
	unit, timer, err := RetentionUnits("/opt/tantor-agent/bin/tantor-agent", "/var/log/tantor-agent", "agent.log", "tantor-agent")
	if err != nil || !strings.Contains(unit, "--prune-logs") || !strings.Contains(timer, "Persistent=true") {
		t.Fatal(unit, timer, err)
	}
}
func TestRejectsSymlinkArchivesAndLogFiles(t *testing.T) {
	dir := t.TempDir()
	elsewhere := t.TempDir()
	if err := os.Symlink(elsewhere, filepath.Join(dir, "archive")); err != nil {
		t.Skip(err)
	}
	if _, err := New(dir, "agent.log"); err == nil {
		t.Fatal("symlink archive accepted")
	}
	if err := Prune(dir, "agent.log", time.Now()); err == nil {
		t.Fatal("symlink cleanup accepted")
	}
}
