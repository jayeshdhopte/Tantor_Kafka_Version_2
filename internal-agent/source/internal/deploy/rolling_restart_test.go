package deploy

import (
	"context"
	"encoding/json"
	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/pkg/api"
	"testing"
)

func TestKafkaServiceProbeNeverMutatesAndRejectsOtherServices(t *testing.T) {
	for _, name := range []string{"broker", "controller", "kafka", "schema-registry", "broker;id", "--now", ""} {
		exec := &commandCaptureExecutor{output: "ActiveState=active\nMainPID=123\nExecMainStartTimestampMonotonic=999\n"}
		engine := NewEngine(&config.Config{}, nil, exec)
		result, err := engine.Execute(context.Background(), &api.Task{Command: "VERIFY_KAFKA_SERVICE", Parameters: map[string]string{"service_name": name}})
		allowed := name == "broker" || name == "controller" || name == "kafka"
		if err != nil || (result.Status == "SUCCESS") != allowed {
			t.Fatalf("%q: %+v %v", name, result, err)
		}
		if allowed {
			if len(exec.commands) != 1 || exec.commands[0].name != "systemctl" || exec.commands[0].args[0] != "show" {
				t.Fatalf("probe mutated service: %+v", exec.commands)
			}
		} else if len(exec.commands) != 0 {
			t.Fatalf("unsupported service reached executor: %q", name)
		}
	}
}

func TestKafkaServiceProbeReportsInactiveWithoutRestart(t *testing.T) {
	exec := &commandCaptureExecutor{output: "ActiveState=inactive\nMainPID=0\nExecMainStartTimestampMonotonic=0\n"}
	engine := NewEngine(&config.Config{}, nil, exec)
	result, _ := engine.verifyKafkaService(context.Background(), &api.Task{Parameters: map[string]string{"service_name": "broker"}})
	var observed map[string]string
	_ = json.Unmarshal([]byte(result.LogOutput), &observed)
	if result.Status != "SUCCESS" || observed["state"] != "inactive" {
		t.Fatalf("unexpected probe result: %+v", result)
	}
}
