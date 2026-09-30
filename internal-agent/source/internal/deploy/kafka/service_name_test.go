package kafka

import (
	"io.translab/tantor-agent/pkg/api"
	"testing"
)

func TestGeneratedKafkaServiceName(t *testing.T) {
	for _, key := range []string{"service_name", "systemd_service"} {
		task := &api.Task{Parameters: map[string]string{key: "tantor-kafka-1.service", "service_role": "broker_controller"}}
		if err := validateCommandTaskInputs(task); err != nil {
			t.Fatal(err)
		}
		if got := serviceNameForTask(task); got != "tantor-kafka-1" {
			t.Fatalf("resolved %q", got)
		}
	}
	task := &api.Task{Parameters: map[string]string{"service_name": "tantor-kafka-1", "systemd_service": "kafka"}}
	if err := validateCommandTaskInputs(task); err != nil {
		t.Fatal(err)
	}
	if got := serviceNameForTask(task); got != "kafka" {
		t.Fatalf("explicit unit precedence changed: %q", got)
	}
}
