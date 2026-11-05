//go:build linux

package readiness

import (
	"context"
	"fmt"
	"io.translab/tantor-agent/internal/executor"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimeParcelAccessRequiresTraversableAncestors(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}
	if _, err := user.Lookup("nobody"); err != nil {
		t.Skip("nobody unavailable")
	}
	root, err := os.MkdirTemp("/var/tmp", "tantor-parcel-access-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	for _, name := range []string{"bin", "libs"} {
		if err := os.Mkdir(filepath.Join(root, name), 0755); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"kafka-storage.sh", "kafka-run-class.sh", "kafka-server-start.sh"} {
		if err := os.WriteFile(filepath.Join(root, "bin", name), []byte("#!/bin/sh\n"), 0755); err != nil {
			t.Fatal(err)
		}
	}
	jar := filepath.Join(root, "libs", "kafka.jar")
	if err := os.WriteFile(jar, []byte("fixture"), 0644); err != nil {
		t.Fatal(err)
	}
	e := executor.New(executor.Options{PrivilegeMode: "direct"})
	if runtimeParcelAccess(context.Background(), root, jar, "nobody", e) == nil {
		t.Fatal("private ancestor incorrectly accessible")
	}
	if err := os.Chmod(root, 0755); err != nil {
		t.Fatal(err)
	}
	if err := runtimeParcelAccess(context.Background(), root, jar, "nobody", e); err != nil {
		t.Fatal(err)
	}
}

type serviceObservationExecutor struct{ path string }

func (e serviceObservationExecutor) Run(ctx context.Context, command string, args ...string) (string, string, error) {
	if command != "bash" || len(args) < 2 || !strings.Contains(args[1], "Existing Kafka service or process detected") {
		return "", "", fmt.Errorf("not measured")
	}
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Env = append(os.Environ(), "PATH="+e.path+":/usr/bin:/bin")
	out, err := cmd.CombinedOutput()
	return string(out), "", err
}
func (e serviceObservationExecutor) RunSudo(context.Context, string, ...string) (string, string, error) {
	panic("readiness mutation")
}

func TestKafkaUnitDetectionDistinguishesSystemMessageBus(t *testing.T) {
	for _, kafkaPresent := range []bool{false, true} {
		t.Run(fmt.Sprint(kafkaPresent), func(t *testing.T) {
			dir := t.TempDir()
			units := "dbus-broker.service enabled enabled\n"
			if kafkaPresent {
				units += "broker.service disabled disabled\n"
			}
			for name, body := range map[string]string{"systemctl": "#!/bin/sh\nprintf '%s' '" + units + "'\n", "pgrep": "#!/bin/sh\nexit 1\n"} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0755); err != nil {
					t.Fatal(err)
				}
			}
			report := Inspect(context.Background(), Request{HostIP: "127.0.0.1", BrokerPort: "0"}, "candidate", serviceObservationExecutor{dir})
			for _, check := range report.Checks {
				if check.Code == "C12" {
					expected := "PASS"
					if kafkaPresent {
						expected = "FAIL"
					}
					if check.Status != expected {
						t.Fatalf("Kafka present=%v: %s, observed=%v", kafkaPresent, check.Status, check.Observed)
					}
					return
				}
			}
			t.Fatal("C12 evidence missing")
		})
	}
}
