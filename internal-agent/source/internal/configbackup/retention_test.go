package configbackup

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRetentionUsesCheckpointAgeAndPreservesUnmanagedFiles(t *testing.T) {
	root := t.TempDir()
	registry := filepath.Join(root, "index")
	dir := filepath.Join(root, "config", "config-backup", "broker.properties")
	for _, p := range []string{registry, dir} {
		if err := os.MkdirAll(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(registry, "broker.path"), []byte(dir), 0600); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	old := "v7-" + now.Add(-Retention-time.Second).Format(LegacyTimestamp) + ".bak"
	snapshot := "v7-" + now.Add(-Retention-time.Second).Format(LegacyTimestamp) + ".dynamic.json"
	boundary := "v7-" + now.Add(-Retention).Format(LegacyTimestamp) + ".bak"
	fresh := "v7-" + now.Format(LegacyTimestamp) + ".bak"
	for _, name := range []string{old, snapshot, boundary, fresh, "operator-backup.bak"} {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("config"), 0600); err != nil {
			t.Fatal(err)
		}
		// Original configs can have ancient mtimes; checkpoint names govern retention.
		os.Chtimes(p, now.Add(-30*24*time.Hour), now.Add(-30*24*time.Hour))
	}
	if err := Sweep(registry, now); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{old, snapshot} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("expired checkpoint retained: %s", name)
		}
	}
	for _, name := range []string{boundary, fresh, "operator-backup.bak"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("valid/manual checkpoint removed: %s", name)
		}
	}
}

func TestRetentionRejectsRedirectedDirectoryAndSkipsFileLinks(t *testing.T) {
	root := t.TempDir()
	registry := filepath.Join(root, "index")
	dir := filepath.Join(root, "config-backup", "broker.properties")
	target := filepath.Join(root, "preserve")
	for _, p := range []string{registry, dir, target} {
		os.MkdirAll(p, 0700)
	}
	now := time.Now().UTC()
	name := "v1-" + now.Add(-8*24*time.Hour).Format(LegacyTimestamp) + ".bak"
	file := filepath.Join(target, name)
	os.WriteFile(file, []byte("keep"), 0600)
	if err := os.Symlink(file, filepath.Join(dir, name)); err != nil {
		t.Skip(err)
	}
	os.WriteFile(filepath.Join(registry, "broker.path"), []byte(dir), 0600)
	if err := Sweep(registry, now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(dir, name)); err != nil {
		t.Fatal("checkpoint link removed")
	}
	redirected := filepath.Join(root, "config-backup", "controller.properties")
	os.Symlink(target, redirected)
	os.WriteFile(filepath.Join(registry, "controller.path"), []byte(redirected), 0600)
	if err := Sweep(registry, now); err == nil {
		t.Fatal("redirected backup directory accepted")
	}
	if _, err := os.Stat(file); err != nil {
		t.Fatal("unrelated target removed")
	}
}

// Legacy registered checkpoints still expire after upgrading the agent.
func TestRetentionIncludesLegacyHiddenBackups(t *testing.T) {
	root := t.TempDir()
	registry := filepath.Join(root, "index")
	dir := filepath.Join(root, ".tantor-backups", "controller.properties")
	for _, path := range []string{registry, dir} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	old := filepath.Join(dir, "v1-"+now.Add(-8*24*time.Hour).Format(LegacyTimestamp)+".bak")
	if err := os.WriteFile(old, []byte("preserved"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(registry, "legacy.path"), []byte(dir), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Sweep(registry, now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("legacy checkpoint did not expire")
	}
}

func TestTimestampNamedBackupsAndSnapshotsExpireAlongsideLegacyFiles(t *testing.T) {
	root := t.TempDir()
	registry := filepath.Join(root, "index")
	dir := filepath.Join(root, "config-backup", "broker.properties")
	for _, path := range []string{registry, dir} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(registry, "broker.path"), []byte(dir), 0600); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	oldStamp := now.Add(-Retention - time.Second).Format(LegacyTimestamp)
	expired := []string{"broker.properties.bak." + oldStamp, "broker.properties.bak." + oldStamp + ".dynamic.json", "vjob-uuid-" + oldStamp + ".bak"}
	preserved := []string{"broker.properties.bak." + now.Add(-Retention).Format(LegacyTimestamp), "broker.properties.bak." + now.Format(LegacyTimestamp), "controller.properties.bak." + oldStamp, "operator-backup.bak"}
	for _, name := range append(expired, preserved...) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("config"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := Sweep(registry, now); err != nil {
		t.Fatal(err)
	}
	for _, name := range expired {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("expired file remains: %s", name)
		}
	}
	for _, name := range preserved {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("unmanaged/recent file removed: %s", name)
		}
	}
}

func TestFilenameUsesOriginalConfigNameAndISTSeconds(t *testing.T) {
	created := time.Date(2026, 10, 13, 13, 53, 2, 123456789, time.UTC)
	for _, name := range []string{"broker.properties", "controller.properties", "server.properties"} {
		want := name + ".bak.20261013T192302"
		if actual := Filename(filepath.Join("config", name), created); actual != want {
			t.Fatalf("got %s want %s", actual, want)
		}
	}
}

func TestISTBackupsCollisionsAndRetention(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "config-backup", "broker.properties")
	registry := filepath.Join(root, "index")
	for _, p := range []string{dir, registry} {
		if err := os.MkdirAll(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	os.WriteFile(filepath.Join(registry, "test.path"), []byte(dir), 0600)
	now := time.Date(2026, 10, 14, 12, 0, 0, 500000000, time.UTC)
	old := now.Add(-Retention - time.Second)
	first, err := ReservePath(dir, "broker.properties", old)
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(first, []byte("first"), 0600)
	second, err := ReservePath(dir, "broker.properties", old)
	if err != nil {
		t.Fatal(err)
	}
	if second != first+".1" {
		t.Fatalf("collision path %s", second)
	}
	data, _ := os.ReadFile(first)
	if string(data) != "first" {
		t.Fatal("backup overwritten")
	}
	os.WriteFile(second+".dynamic.json", []byte("snapshot"), 0600)
	boundary, err := ReservePath(dir, "broker.properties", now.Add(-Retention))
	if err != nil {
		t.Fatal(err)
	}
	offset := filepath.Join(dir, "broker.properties.bak."+old.In(IST).Format("20060102T150405.000000000-0700"))
	os.WriteFile(offset, []byte("legacy offset backup"), 0600)
	if err := Sweep(registry, now); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{first, second, second + ".dynamic.json", offset} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("expired file remains: %s", path)
		}
	}
	if _, err := os.Stat(boundary); err != nil {
		t.Fatal("retention boundary removed", err)
	}
}
