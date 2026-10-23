package artifactwork

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrivateWorkspacesCleanupOnlyTheirCluster(t *testing.T) {
	root := t.TempDir()
	first, err := New(root, Owner{ClusterID: "a", TaskID: "task-1"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(root, Owner{ClusterID: "a", TaskID: "task-2"})
	if err != nil {
		t.Fatal(err)
	}
	other, err := New(root, Owner{ClusterID: "b", TaskID: "task-3"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Dir() == second.Dir() {
		t.Fatal("concurrent tasks share staging")
	}
	for _, dir := range []string{first.Dir(), second.Dir(), other.Dir()} {
		os.WriteFile(filepath.Join(dir, "package.tgz"), []byte("archive"), 0600)
	}
	shared := filepath.Join(root, "parcel_KAFKA_4.3.0.tgz")
	os.WriteFile(shared, []byte("shared"), 0600)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal("retry", err)
	}
	second.release() // simulate a process crash: files remain, OS lock is released
	if err := CleanupCluster(root, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(second.Dir()); !os.IsNotExist(err) {
		t.Fatal("abandoned task archive retained")
	}
	if _, err := os.Stat(other.Dir()); err != nil {
		t.Fatal("other cluster workspace removed", err)
	}
	if body, _ := os.ReadFile(shared); string(body) != "shared" {
		t.Fatal("shared parcel cache changed")
	}
}
func TestCleanupDoesNotFollowRedirectedWorkspace(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	keep := filepath.Join(outside, "keep")
	os.WriteFile(keep, []byte("keep"), 0600)
	workspace, err := New(root, Owner{ClusterID: "a", TaskID: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if err := workspace.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, workspace.Dir()); err != nil {
		t.Skip("symlink permission unavailable", err)
	}
	if workspace.Close() == nil {
		t.Fatal("redirected staging accepted")
	}
	if err := CleanupCluster(root, "a"); err != nil {
		t.Fatal(err)
	}
	if body, _ := os.ReadFile(keep); string(body) != "keep" {
		t.Fatal("followed symlink")
	}
}
