package kafka

import (
	"bytes"
	"strings"
	"testing"
	"text/template"
)

func TestHeapSizesForTask(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]string
		xms    string
		xmx    string
	}{
		{"separate bounds", map[string]string{"heap_size": "6G", "heap_xms": "4G", "heap_xmx": "6G"}, "4G", "6G"},
		{"legacy size", map[string]string{"heap_size": "2G"}, "2G", "2G"},
		{"default", map[string]string{}, "1G", "1G"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			xms, xmx := heapSizesForTask(test.params)
			if xms != test.xms || xmx != test.xmx {
				t.Fatalf("heap bounds = %s/%s, want %s/%s", xms, xmx, test.xms, test.xmx)
			}
		})
	}
}

func TestServiceTemplatesUseSeparateHeapBounds(t *testing.T) {
	props := struct {
		User, Group, JavaHome, InstallDir, HeapXms, HeapXmx         string
		JmxAgentPath, JmxPort, JmxConfigPath, AppLogDir, ConfigPath string
	}{HeapXms: "4G", HeapXmx: "6G"}
	for _, source := range []string{SystemdTemplate, ZooKeeperSystemdTemplate} {
		var output bytes.Buffer
		if err := template.Must(template.New("service").Parse(source)).Execute(&output, props); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(output.String(), "KAFKA_HEAP_OPTS=-Xmx6G -Xms4G") {
			t.Fatalf("service template did not preserve heap range: %s", output.String())
		}
	}
}

func TestKafkaServiceDoesNotCallBroadStopScript(t *testing.T) {
	if strings.Contains(SystemdTemplate, "ExecStop=") || strings.Contains(SystemdTemplate, "kafka-server-stop.sh") {
		t.Fatal("Kafka systemd service must not invoke the Kafka-wide stop script")
	}
	if !strings.Contains(SystemdTemplate, "ExecStart={{.InstallDir}}/bin/kafka-server-start.sh {{.ConfigPath}}") {
		t.Fatal("Kafka systemd service must still start its configured process")
	}
}
