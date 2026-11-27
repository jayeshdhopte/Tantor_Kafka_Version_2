package javaupgrade

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestRuntimeOverridePreservesAndRestoresPinnedConfiguration(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "new override removed", true: "existing override restored"}[existing], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "custom-controller.service.d", "zzzz-tantor-java-upgrade.conf")
			old := []byte("[Service]\nEnvironment=JAVA_HOME=/old\n")
			oldEnv := []byte("JAVA_HOME=/previous\n")
			saved := []runtimeOverride{{path: path, original: old, existed: existing, envOriginal: oldEnv, envExisted: existing}}
			run := func(_ context.Context, cmd string, args ...string) (string, error) {
				switch cmd {
				case "mkdir":
					return "", os.MkdirAll(args[len(args)-1], 0755)
				case "install":
					data, e := os.ReadFile(args[len(args)-2])
					if e != nil {
						return "", e
					}
					return "", os.WriteFile(args[len(args)-1], data, 0644)
				case "rm":
					e := os.Remove(args[len(args)-1])
					if os.IsNotExist(e) {
						e = nil
					}
					return "", e
				}
				return "", nil
			}
			if e := applyRuntimeOverrides(context.Background(), run, saved, "/target/jdk"); e != nil {
				t.Fatal(e)
			}
			data, e := os.ReadFile(path + ".env")
			if e != nil || string(data) != "JAVA_HOME=/target/jdk\nJRE_HOME=/target/jdk\n" {
				t.Fatalf("bad target runtime: %s %v", data, e)
			}
			if e := restoreRuntimeOverrides(context.Background(), run, saved); e != nil {
				t.Fatal(e)
			}
			for _, f := range []struct {
				p    string
				want []byte
			}{{path, old}, {path + ".env", oldEnv}} {
				data, e := os.ReadFile(f.p)
				if existing && (e != nil || string(data) != string(f.want)) {
					t.Fatalf("lost original runtime: %v", e)
				}
				if !existing && !os.IsNotExist(e) {
					t.Fatal("new runtime override retained")
				}
			}
		})
	}
}
