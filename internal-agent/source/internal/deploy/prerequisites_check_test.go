package deploy

import (
	"context"
	"errors"
	"strings"
	"testing"

	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/pkg/api"
)

type prerequisiteFakeExecutor struct {
	outputs []string
	failAt  int
	calls   int
}

func (f *prerequisiteFakeExecutor) Run(_ context.Context, _ string, _ ...string) (string, string, error) {
	f.calls++
	idx := f.calls - 1
	out := "ok"
	if idx < len(f.outputs) {
		out = f.outputs[idx]
	}
	if f.failAt == f.calls {
		return out, "", errors.New("check failed")
	}
	return out, "", nil
}

func (f *prerequisiteFakeExecutor) RunSudo(_ context.Context, _ string, _ ...string) (string, string, error) {
	return "", "", errors.New("RunSudo should not be used by CHECK_PREREQUISITES")
}

func newPrerequisiteTestEngine(exec *prerequisiteFakeExecutor) *Engine {
	return NewEngine(&config.Config{Agent: config.AgentConfig{HostID: "host-208"}}, nil, exec)
}

func TestCheckPrerequisitesAllPass(t *testing.T) {
	exec := &prerequisiteFakeExecutor{outputs: []string{
		"1024000/1024000",
		"0",
		"always madvise [never]",
		"Permissive",
		"17.0.15",
		"chronyd: Active",
	}}
	engine := newPrerequisiteTestEngine(exec)

	result, err := engine.checkPrerequisites(context.Background(), &api.Task{TaskID: "task-1"})
	if err != nil {
		t.Fatalf("checkPrerequisites returned error: %v", err)
	}
	if result.Status != "SUCCESS" {
		t.Fatalf("status = %q, want SUCCESS; result=%+v", result.Status, result)
	}
	if exec.calls != 6 {
		t.Fatalf("executor calls = %d, want 6", exec.calls)
	}
	for _, expected := range []string{
		"Open file limit (soft/hard): 1024000/1024000 [Pass]",
		"Swappiness: 0 [Pass]",
		"Transparent Huge Pages: always madvise [never] [Pass]",
		"SELinux: Permissive [Pass]",
		"Java Version: 17.0.15 [Pass]",
		"NTP Service: chronyd: Active [Pass]",
		"Summary: 6 passed, 0 failed, 6 total",
	} {
		if !strings.Contains(result.LogOutput, expected) {
			t.Fatalf("log output missing %q:\n%s", expected, result.LogOutput)
		}
	}
}

func TestCheckPrerequisitesJavaFailureStillChecksEverything(t *testing.T) {
	exec := &prerequisiteFakeExecutor{
		outputs: []string{
			"1024000/1024000",
			"0",
			"always madvise [never]",
			"Disabled",
			"21.0.7",
			"ntpd: Active",
		},
	}
	engine := newPrerequisiteTestEngine(exec)

	result, err := engine.checkPrerequisites(context.Background(), &api.Task{TaskID: "task-2"})
	if err != nil {
		t.Fatalf("checkPrerequisites returned error: %v", err)
	}
	if result.Status != "FAILED" {
		t.Fatalf("status = %q, want FAILED; result=%+v", result.Status, result)
	}
	if exec.calls != 6 {
		t.Fatalf("executor calls = %d, want 6 (checks must not stop at first failure)", exec.calls)
	}
	if !strings.Contains(result.LogOutput, "Java Version: 21.0.7 [Fail, must be 17.x]") {
		t.Fatalf("expected Java 17-only failure in log:\n%s", result.LogOutput)
	}
	if !strings.Contains(result.LogOutput, "NTP Service: ntpd: Active [Pass]") {
		t.Fatalf("expected later NTP check to run:\n%s", result.LogOutput)
	}
	if result.ErrorMsg != "Kafka prerequisite check failed: 1 of 6 required checks failed" {
		t.Fatalf("unexpected error message: %q", result.ErrorMsg)
	}
}
