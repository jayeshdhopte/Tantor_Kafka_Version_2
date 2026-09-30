package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestExtractKafkaAppLogDir(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "broker JVM option",
			input: `java -Dlog4j.configuration=file:/opt/kafka/config/log4j2.yaml -Dkafka.logs.dir=/opt/apache/log/kafka-broker kafka.Kafka`,
			want:  "/opt/apache/log/kafka-broker",
		},
		{
			name:  "quoted controller systemd environment",
			input: `Environment="KAFKA_LOG4J_OPTS=-Dlog4j.configuration=file:/opt/kafka/config/controller-log4j2.yaml -Dkafka.logs.dir=/opt/apache/log/kafka-controller"`,
			want:  "/opt/apache/log/kafka-controller",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractKafkaAppLogDir(tt.input); got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestSystemdKafkaAppLogDirFallbacks(t *testing.T) {
	binDir := t.TempDir()
	systemctl := filepath.Join(binDir, "systemctl")
	script := `#!/bin/sh
case "${FAKE_SYSTEMD_MODE:-}:${1:-}" in
  environment:show)
    printf '%s\n' 'KAFKA_LOG4J_OPTS=-Dlog4j.configuration=file:/opt/kafka/config/log4j2.yaml -Dkafka.logs.dir=/opt/apache/log/kafka-broker'
    ;;
  unit:cat)
    printf '%s\n' 'Environment="KAFKA_LOG4J_OPTS=-Dlog4j.configuration=file:/opt/kafka/config/controller-log4j2.yaml -Dkafka.logs.dir=/opt/apache/log/kafka-controller"'
    ;;
esac
`
	if err := os.WriteFile(systemctl, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	t.Run("ExecStart wins", func(t *testing.T) {
		got := systemdKafkaAppLogDir(
			"broker.service",
			"java -Dkafka.logs.dir=/opt/apache/log/from-exec-start kafka.Kafka",
		)
		if got != "/opt/apache/log/from-exec-start" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("systemd Environment fallback", func(t *testing.T) {
		t.Setenv("FAKE_SYSTEMD_MODE", "environment")
		if got := systemdKafkaAppLogDir("broker.service", "java kafka.Kafka"); got != "/opt/apache/log/kafka-broker" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("systemd unit fallback", func(t *testing.T) {
		t.Setenv("FAKE_SYSTEMD_MODE", "unit")
		if got := systemdKafkaAppLogDir("controller.service", "java kafka.Kafka"); got != "/opt/apache/log/kafka-controller" {
			t.Fatalf("got %q", got)
		}
	})
}
