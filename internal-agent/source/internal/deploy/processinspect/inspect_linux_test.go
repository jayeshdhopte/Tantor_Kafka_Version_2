//go:build linux

package processinspect

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBoundedReadRejectsSymlinkFileAndAncestorWithoutChangingData(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "real")
	if err := os.Mkdir(real, 0700); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(real, "broker.properties")
	const original = "node.id=1\nprocess.roles=broker\n"
	if err := os.WriteFile(p, []byte(original), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readRegular(p, 4096); err != nil {
		t.Fatal(err)
	}
	if _, err := readRegular(p, 2); err == nil {
		t.Fatal("oversized file accepted")
	}
	link := filepath.Join(dir, "linked.properties")
	if err := os.Symlink(p, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readRegular(link, 4096); err == nil {
		t.Fatal("symlink file followed")
	}
	ancestor := filepath.Join(dir, "linked-directory")
	if err := os.Symlink(real, ancestor); err != nil {
		t.Fatal(err)
	}
	if _, err := readRegular(filepath.Join(ancestor, "broker.properties"), 4096); err == nil {
		t.Fatal("symlink ancestor followed")
	}
	after, err := os.ReadFile(p)
	if err != nil || string(after) != original {
		t.Fatal("observation altered file")
	}
}
