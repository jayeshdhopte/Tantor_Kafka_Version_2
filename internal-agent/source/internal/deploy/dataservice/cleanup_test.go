package dataservice

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type cleanupExecutor struct {
	unit     string
	commands []string
	failDir  bool
}

func (e *cleanupExecutor) Run(_ context.Context, name string, args ...string) (string, string, error) {
	if name == "readlink" {
		return args[len(args)-1], "", nil
	}
	return "", "", nil
}

func (e *cleanupExecutor) RunSudo(_ context.Context, name string, args ...string) (string, string, error) {
	command := name + " " + strings.Join(args, " ")
	e.commands = append(e.commands, command)
	if name == "test" && len(args) > 0 && args[0] == "-f" && e.unit == "" {
		return "", "", errors.New("not found")
	}
	if name == "cat" {
		return e.unit, "", nil
	}
	if e.failDir && strings.HasPrefix(command, "rm -rf") {
		return "", "", errors.New("remove failed")
	}
	return "", "", nil
}

func TestCleanupRemovesOnlyMatchingManagedService(t *testing.T) {
	for _, kind := range []string{Schema, Connect} {
		t.Run(kind, func(t *testing.T) {
			task := taskFor()
			s, err := Parse(kind, task)
			if err != nil {
				t.Fatal(err)
			}
			unit := ConfigFiles(s, "/usr/lib/jvm/java-17")[2].Content
			exec := &cleanupExecutor{unit: unit}
			if _, err := Clean(context.Background(), exec, kind, task); err != nil {
				t.Fatal(err)
			}
			commands := strings.Join(exec.commands, "\n")
			for _, required := range []string{"systemctl stop " + kind + ".service", "systemctl disable " + kind + ".service",
				"rm -rf -- " + s.Install, "rm -f -- /etc/systemd/system/" + kind + ".service", "systemctl daemon-reload"} {
				if !strings.Contains(commands, required) {
					t.Fatalf("missing %q in commands:\n%s", required, commands)
				}
			}
		})
	}
}

func TestCleanupSkipsAbsentUnitAndRejectsForeignUnit(t *testing.T) {
	task := taskFor()
	absent := &cleanupExecutor{}
	if _, err := Clean(context.Background(), absent, Connect, task); err != nil {
		t.Fatal(err)
	}
	if len(absent.commands) != 1 {
		t.Fatalf("absent unit caused mutation: %v", absent.commands)
	}
	foreign := &cleanupExecutor{unit: "Description=Someone else's service\n"}
	if _, err := Clean(context.Background(), foreign, Connect, task); err == nil {
		t.Fatal("accepted foreign service unit")
	}
	if strings.Contains(strings.Join(foreign.commands, "\n"), "systemctl stop") {
		t.Fatalf("foreign service was stopped: %v", foreign.commands)
	}
}

func TestCleanupFailureLeavesUnitForRetry(t *testing.T) {
	task := taskFor()
	s, err := Parse(Connect, task)
	if err != nil {
		t.Fatal(err)
	}
	exec := &cleanupExecutor{unit: ConfigFiles(s, "/usr/lib/jvm/java-17")[2].Content, failDir: true}
	if _, err := Clean(context.Background(), exec, Connect, task); err == nil {
		t.Fatal("expected directory cleanup failure")
	}
	if strings.Contains(strings.Join(exec.commands, "\n"), "rm -f -- /etc/systemd/system/") {
		t.Fatal("unit removed before cleanup completed")
	}
}
