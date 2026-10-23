package logrotation

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestApplicationRetentionCleanupOwnershipAndRetry(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux systemd deployment shell")
	}
	for _, mode := range []string{"complete", "timer-only", "service-only", "foreign", "wrong-path", "stop-failure", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			bin := filepath.Join(root, "bin")
			os.Mkdir(bin, 0755)
			calls := filepath.Join(root, "calls")
			stub := `#!/bin/sh
echo "$*" >> "$CALLS"
if [ "$1" = show ]; then echo loaded; fi
if [ "$1" = stop ] && [ "$FAIL_STOP" = yes ]; then exit 1; fi
`
			os.WriteFile(filepath.Join(bin, "systemctl"), []byte(stub), 0755)
			prefix := "tantor-kafka-broker"
			service, timer, err := RetentionUnits("/opt/agent", "/var/log/tantor/kafka-broker", "broker.log", prefix)
			if err != nil {
				t.Fatal(err)
			}
			servicePath := filepath.Join(root, prefix+"-log-retention.service")
			timerPath := filepath.Join(root, prefix+"-log-retention.timer")
			if mode == "foreign" {
				service = "Description=foreign service\n"
			}
			if mode == "wrong-path" {
				service = strings.ReplaceAll(service, "/var/log/tantor/kafka-broker", "/some/other/cluster")
			}
			if mode != "timer-only" {
				os.WriteFile(servicePath, []byte(service), 0644)
			}
			if mode != "service-only" {
				os.WriteFile(timerPath, []byte(timer), 0644)
			}
			if mode == "symlink" {
				os.Remove(servicePath)
				os.Symlink(timerPath, servicePath)
			}
			unrelated := filepath.Join(root, "tantor-discovery-agent-log-retention.timer")
			os.WriteFile(unrelated, []byte("preserve"), 0644)
			command := func() error {
				cmd := exec.Command("bash", "-c", removeApplicationRetentionShell, "--", root, prefix, "broker.log", "/var/log/tantor/kafka-controller", "/var/log/tantor/kafka-broker")
				fail := "no"
				if mode == "stop-failure" {
					fail = "yes"
				}
				cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "CALLS="+calls, "FAIL_STOP="+fail)
				return cmd.Run()
			}
			err = command()
			success := mode == "complete" || mode == "timer-only" || mode == "service-only"
			if (err == nil) != success {
				t.Fatalf("unexpected cleanup result: %v", err)
			}
			if success {
				if _, err := os.Lstat(timerPath); !os.IsNotExist(err) {
					t.Fatal("timer retained")
				}
				if _, err := os.Lstat(servicePath); !os.IsNotExist(err) {
					t.Fatal("service retained")
				}
				if err := command(); err != nil {
					t.Fatal("retry", err)
				}
			} else {
				if _, err := os.Lstat(servicePath); err != nil {
					t.Fatal("cleanup evidence lost", err)
				}
			}
			if body, _ := os.ReadFile(unrelated); string(body) != "preserve" {
				t.Fatal("shared agent retention changed")
			}
			body, _ := os.ReadFile(calls)
			if (mode == "foreign" || mode == "wrong-path" || mode == "symlink") && len(body) > 0 {
				t.Fatal("mutated foreign service", string(body))
			}
			if mode == "complete" && strings.Index(string(body), "stop "+prefix+"-log-retention.timer") > strings.Index(string(body), "stop "+prefix+"-log-retention.service") {
				t.Fatal("pruner stopped before timer")
			}
		})
	}
}
