package kafka

import (
	"strings"
	"testing"
)

func TestKafkaForegroundServiceCapturesBothJournalStreams(t *testing.T) {
	for _, line := range []string{"StandardOutput=journal\n", "StandardError=journal\n", "ExecStart={{.InstallDir}}/bin/kafka-server-start.sh {{.ConfigPath}}\n"} {
		if !strings.Contains(SystemdTemplate, line) {
			t.Fatalf("Kafka journal contract missing %q", line)
		}
	}
	if strings.Contains(SystemdTemplate, "-daemon") || strings.Contains(SystemdTemplate, "StandardOutput=null") {
		t.Fatal("Kafka bypasses systemd output capture")
	}
}
