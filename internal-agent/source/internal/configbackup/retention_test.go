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
	old := "v7-" + now.Add(-Retention-time.Second).Format(Timestamp) + ".bak"
	snapshot := "v7-" + now.Add(-Retention-time.Second).Format(Timestamp) + ".dynamic.json"
	boundary := "v7-" + now.Add(-Retention).Format(Timestamp) + ".bak"
	fresh := "v7-" + now.Format(Timestamp) + ".bak"
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
	name := "v1-" + now.Add(-8*24*time.Hour).Format(Timestamp) + ".bak"
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
	old := filepath.Join(dir, "v1-"+now.Add(-8*24*time.Hour).Format(Timestamp)+".bak")
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
