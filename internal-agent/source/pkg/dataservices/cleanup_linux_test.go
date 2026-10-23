//go:build linux

package dataservices

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"io.translab/tantor-agent/pkg/logrotation"
)

type systemdCleanupExecutor struct{}

func (systemdCleanupExecutor) Run(ctx context.Context, name string, args ...string) (string, string, error) {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	return string(out), "", err
}
func (e systemdCleanupExecutor) RunSudo(ctx context.Context, name string, args ...string) (string, string, error) {
	return e.Run(ctx, name, args...)
}

// Opt-in only: creates unique disposable units, never operates on an existing
// Kafka/SR/Connect deployment. This exercises the same public bridge as discovery.
func TestSystemdCleanupStopsProcessesAndRemovesOwnedResources(t *testing.T) {
	if os.Getenv("TANTOR_TEST_SYSTEMD_CLEANUP") != "1" {
		t.Skip("opt-in disposable systemd integration test")
	}
	if os.Geteuid() != 0 {
		t.Fatal("requires root on an isolated Linux test node")
	}
	root, err := os.MkdirTemp("/var/tmp", "tantor-cleanup-qa-")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(root, "/var/tmp/tantor-cleanup-qa-") {
		t.Fatal("unexpected test directory")
	}
	name := "kafka-connect-qa-" + strings.TrimPrefix(filepath.Base(root), "tantor-cleanup-qa-")
	prefix := "tantor-" + name
	names := []string{name + ".service", prefix + "-log-retention.timer", prefix + "-log-retention.service"}
	command := func(args ...string) string {
		t.Helper()
		out, err := exec.Command("systemctl", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("systemctl %v: %v %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	t.Cleanup(func() {
		for _, unit := range names {
			exec.Command("systemctl", "stop", unit).Run()
			exec.Command("systemctl", "disable", unit).Run()
			os.Remove(filepath.Join("/etc/systemd/system", unit))
			os.RemoveAll(filepath.Join("/etc/systemd/system", unit+".d"))
			exec.Command("systemctl", "reset-failed", unit).Run()
		}
		exec.Command("systemctl", "daemon-reload").Run()
		os.RemoveAll(root)
	})
	install, config, logs, work, plugins := root+"/runtime", root+"/config", root+"/logs", root+"/work", root+"/plugins"
	for _, dir := range []string{install + "/bin", config, logs, work, plugins} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}
	keep := root + "/unrelated"
	os.WriteFile(keep, []byte("preserve"), 0600)
	launcher := install + "/bin/connect-distributed.sh"
	os.WriteFile(launcher, []byte("#!/bin/sh\nsleep 300 &\necho $! > "+work+"/child.pid\nwait\n"), 0755)
	os.WriteFile(config+"/connect-distributed.properties", []byte("# disposable fixture\n"), 0600)
	unit := fmt.Sprintf("[Unit]\nDescription=Tantor kafka-connect\n[Service]\nType=simple\nExecStart=%s %s/connect-distributed.properties\nTimeoutStopSec=5s\n[Install]\nWantedBy=multi-user.target\n", launcher, config)
	os.WriteFile(filepath.Join("/etc/systemd/system", names[0]), []byte(unit), 0644)
	pruner := root + "/pruner"
	os.WriteFile(pruner, []byte("#!/bin/sh\nsleep 300 &\nwait\n"), 0755)
	service, timer, err := logrotation.RetentionUnits(pruner, logs, name+".log", prefix)
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join("/etc/systemd/system", names[1]), []byte(timer), 0644)
	os.WriteFile(filepath.Join("/etc/systemd/system", names[2]), []byte(service), 0644)
	for _, unit := range names {
		os.MkdirAll(filepath.Join("/etc/systemd/system", unit+".d"), 0755)
		os.WriteFile(filepath.Join("/etc/systemd/system", unit+".d", "qa.conf"), []byte("[Unit]\nDescription=Tantor cleanup QA\n"), 0644)
	}
	command("daemon-reload")
	command("enable", "--now", names[0])
	command("enable", "--now", names[1])
	command("start", "--no-block", names[2])
	var pids []int
	for _, unit := range []string{names[0], names[2]} {
		pid := 0
		deadline := time.Now().Add(5 * time.Second)
		for pid == 0 && time.Now().Before(deadline) {
			pid, _ = strconv.Atoi(command("show", unit, "--property=MainPID", "--value"))
			if pid == 0 {
				time.Sleep(20 * time.Millisecond)
			}
		}
		if pid == 0 {
			t.Fatal("fixture process did not start", unit)
		}
		pids = append(pids, pid)
	}
	child, err := os.ReadFile(work + "/child.pid")
	if err != nil {
		t.Fatal(err)
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(child)))
	pids = append(pids, pid)
	task := &Task{Parameters: map[string]string{"service_name": name, "install_dir": install, "config_dir": config, "log_dir": logs, "working_dir": work, "plugin_dir": plugins, "bootstrap_servers": "127.0.0.1:9092", "kafka_install_dir": "/opt/kafka"}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if output, err := Execute(ctx, "CLEAN", "kafka-connect", "", task, systemdCleanupExecutor{}, nil); err != nil {
		t.Fatal(output, err)
	}
	for _, unit := range names {
		for _, path := range []string{filepath.Join("/etc/systemd/system", unit), filepath.Join("/etc/systemd/system", unit+".d")} {
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				t.Fatal("owned unit retained", path, err)
			}
		}
	}
	for _, dir := range []string{install, config, logs, work, plugins} {
		if _, err := os.Lstat(dir); !os.IsNotExist(err) {
			t.Fatal("owned directory retained", dir, err)
		}
	}
	for _, pid := range pids {
		if pid <= 0 {
			t.Fatal("invalid fixture pid")
		}
		if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); !os.IsNotExist(err) {
			t.Fatal("fixture process survived cleanup", pid)
		}
	}
	if body, _ := os.ReadFile(keep); string(body) != "preserve" {
		t.Fatal("unrelated file changed")
	}
	if _, err := Execute(ctx, "CLEAN", "kafka-connect", "", task, systemdCleanupExecutor{}, nil); err != nil {
		t.Fatal("idempotent retry", err)
	}
}
