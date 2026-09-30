package kafka

import (
	"io.translab/tantor-agent/pkg/api"
	"strings"
	"testing"
)

func TestControllerMonitoringPortAndService(t *testing.T) {
	task := &api.Task{Parameters: map[string]string{"service_role": "controller", "systemd_service": "controller", "jmx_port": "7192", "controller_jmx_port": "7072", "kafka_exporter_artifact_id": "artifact"}}
	if got := kafkaJMXPortForTask(task); got != "7192" {
		t.Fatalf("controller JMX port %s", got)
	}
	for _, service := range deploymentServices(task) {
		if strings.Contains(service, "exporter") {
			t.Fatalf("controller has broker exporter: %v", deploymentServices(task))
		}
	}
	task.Parameters["service_role"] = "broker"
	task.Parameters["jmx_port"] = "7191"
	if got := kafkaJMXPortForTask(task); got != "7191" {
		t.Fatalf("broker JMX port %s", got)
	}
	found := false
	for _, service := range deploymentServices(task) {
		if strings.Contains(service, "exporter") {
			found = true
		}
	}
	if !found {
		t.Fatal("broker exporter omitted")
	}
}
