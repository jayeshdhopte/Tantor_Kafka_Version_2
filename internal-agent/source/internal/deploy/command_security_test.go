package deploy

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/pkg/api"
)

type capturedCommand struct {
	name string
	args []string
}

type commandCaptureExecutor struct {
	commands []capturedCommand
	output   string
}

func (e *commandCaptureExecutor) Run(_ context.Context, name string, args ...string) (string, string, error) {
	e.commands = append(e.commands, capturedCommand{name: name, args: append([]string(nil), args...)})
	return e.output, "", nil
}

func (e *commandCaptureExecutor) RunSudo(_ context.Context, name string, args ...string) (string, string, error) {
	e.commands = append(e.commands, capturedCommand{name: name, args: append([]string(nil), args...)})
	return e.output, "", nil
}

func TestVerifyKRaftQuorumUsesIsolatedArgv(t *testing.T) {
	exec := &commandCaptureExecutor{output: "ClusterId: cluster-1\nLeaderId: 1"}
	engine := NewEngine(&config.Config{Agent: config.AgentConfig{HostID: "host-1"}}, nil, exec)
	task := &api.Task{Parameters: map[string]string{
		"controller_endpoints": "broker.example:9093",
		"kafka_install_dir":    "/opt",
		"cluster_uuid":         "cluster-1",
	}}

	result, err := engine.verifyKRaftQuorum(context.Background(), task)
	if err != nil || result.Status != "SUCCESS" {
		t.Fatalf("verifyKRaftQuorum() = %+v, %v", result, err)
	}
	if len(exec.commands) != 1 {
		t.Fatalf("commands = %+v", exec.commands)
	}
	command := exec.commands[0]
	if command.name != filepath.Join("/opt", "kafka", "bin", "kafka-metadata-quorum.sh") {
		t.Fatalf("executable = %q", command.name)
	}
	wantArgs := []string{"--bootstrap-controller", "broker.example:9093", "describe", "--status"}
	if strings.Join(command.args, "\x00") != strings.Join(wantArgs, "\x00") {
		t.Fatalf("argv = %#v, want %#v", command.args, wantArgs)
	}
	if command.name == "bash" || command.name == "sh" {
		t.Fatalf("unexpected shell command: %+v", command)
	}
}

func TestVerifyKRaftQuorumRejectsInjectionBeforeExecution(t *testing.T) {
	payloads := []string{
		"broker:9093;touch /tmp/pwned", "broker:9093 && id",
		"broker:9093$(id)", "broker:9093`id`", "broker:9093\nwhoami",
	}
	for _, payload := range payloads {
		exec := &commandCaptureExecutor{}
		engine := NewEngine(&config.Config{Agent: config.AgentConfig{HostID: "host-1"}}, nil, exec)
		result, _ := engine.verifyKRaftQuorum(context.Background(), &api.Task{Parameters: map[string]string{
			"controller_endpoints": payload,
			"kafka_install_dir":    "/opt",
		}})
		if result.Status != "FAILED" {
			t.Errorf("payload %q status = %q", payload, result.Status)
		}
		if len(exec.commands) != 0 {
			t.Errorf("payload %q reached executor: %+v", payload, exec.commands)
		}
	}
}

func TestServiceActionRejectsLeadingDash(t *testing.T) {
	exec := &commandCaptureExecutor{}
	engine := NewEngine(&config.Config{Agent: config.AgentConfig{HostID: "host-1"}}, nil, exec)
	result, _ := engine.startService(context.Background(), &api.Task{Parameters: map[string]string{"service_name": "--now"}})
	if result.Status != "FAILED" || len(exec.commands) != 0 {
		t.Fatalf("leading-dash service reached executor: result=%+v commands=%+v", result, exec.commands)
	}
}

func TestTaskControlledDeploymentPathsContainNoShellExecution(t *testing.T) {
	for _, file := range []string{
		filepath.Join("kafka", "deployer.go"),
		filepath.Join("parcel", "deployer.go"),
	} {
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		text := string(source)
		for _, forbidden := range []string{`"bash", "-c"`, `"sh", "-c"`, "shellQuote("} {
			if strings.Contains(text, forbidden) {
				t.Errorf("%s contains forbidden task-controlled shell construction %q", file, forbidden)
			}
		}
	}
}
