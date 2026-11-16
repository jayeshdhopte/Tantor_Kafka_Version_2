package layout

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestVerifyRuntimeIsReadOnlyAndRejectsDrift(t *testing.T) {
	source, installed := t.TempDir(), t.TempDir()
	for _, root := range []string{source, installed} {
		for _, dir := range []string{"bin", "libs"} {
			if err := os.Mkdir(filepath.Join(root, dir), 0755); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(root, "bin", "kafka.sh"), []byte("approved"), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "libs", "kafka.jar"), []byte("bytes"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	if err := VerifyRuntime(ctx, source, installed); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(installed, "libs", "kafka.jar")
	if err := os.WriteFile(target, []byte("drift"), 0644); err != nil {
		t.Fatal(err)
	}
	if VerifyRuntime(ctx, source, installed) == nil {
		t.Fatal("changed bytes accepted")
	}
	b, _ := os.ReadFile(target)
	if string(b) != "drift" {
		t.Fatal("inspection repaired runtime")
	}
	os.Remove(target)
	if VerifyRuntime(ctx, source, installed) == nil {
		t.Fatal("missing runtime accepted")
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatal("inspection created missing file")
	}
	os.WriteFile(target, []byte("bytes"), 0644)
	extra := filepath.Join(installed, "libs", "foreign.jar")
	os.WriteFile(extra, []byte("foreign"), 0644)
	if VerifyRuntime(ctx, source, installed) == nil {
		t.Fatal("extra runtime accepted")
	}
	os.Remove(extra)
	os.Remove(target)
	if err := os.Symlink(filepath.Join(source, "libs", "kafka.jar"), target); err == nil {
		if VerifyRuntime(ctx, source, installed) == nil {
			t.Fatal("linked runtime accepted")
		}
	}
}
