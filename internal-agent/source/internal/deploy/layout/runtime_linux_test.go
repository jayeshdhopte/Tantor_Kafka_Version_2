package layout

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestRuntimePreparationResumesAndRejectsForeignFilesAndSymlinks(t *testing.T) {
	root, e := os.MkdirTemp("/var/tmp", "tantor-layout-test-")
	if e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(root)
	source := filepath.Join(root, "cache")
	dest := filepath.Join(root, "install")
	for _, dir := range []string{"bin", "libs"} {
		if e := os.MkdirAll(filepath.Join(source, dir), 0755); e != nil {
			t.Fatal(e)
		}
	}
	file := filepath.Join(source, "bin", "kafka-server-start.sh")
	os.WriteFile(file, []byte("verified fixture"), 0755)
	if e := PrepareRuntime(context.Background(), source, dest); e != nil {
		t.Fatal(e)
	}
	if e := PrepareRuntime(context.Background(), source, dest); e != nil {
		t.Fatal("resume", e)
	}
	target := filepath.Join(dest, "bin", "kafka-server-start.sh")
	os.WriteFile(target, []byte("foreign bytes"), 0755)
	if e := PrepareRuntime(context.Background(), source, dest); e == nil {
		t.Fatal("overwrote foreign runtime")
	}
	if b, _ := os.ReadFile(target); string(b) != "foreign bytes" {
		t.Fatal("foreign runtime mutated")
	}
	os.Remove(target)
	os.Symlink(file, target)
	if e := PrepareRuntime(context.Background(), source, dest); e == nil {
		t.Fatal("accepted symlink target")
	}
}
