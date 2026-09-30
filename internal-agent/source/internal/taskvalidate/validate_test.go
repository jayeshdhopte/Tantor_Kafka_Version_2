package taskvalidate

import "testing"

func TestHostPort(t *testing.T) {
	valid := []string{"127.0.0.1:9092", "broker.example:9092", "[2001:db8::1]:9093"}
	for _, value := range valid {
		if got, err := HostPort(value); err != nil || got != value {
			t.Errorf("HostPort(%q) = %q, %v", value, got, err)
		}
	}

	invalid := []string{
		"127.0.0.1:9092;touch /tmp/pwned", "localhost:9092$(id)",
		"broker:9092 && id", "broker:9092\nwhoami", "`id`",
		"user@broker:9092", "http://broker:9092", "broker:0", "broker:65536",
		"bad_host:9092", "2001:db8::1:9093",
	}
	for _, value := range invalid {
		if _, err := HostPort(value); err == nil {
			t.Errorf("HostPort(%q) unexpectedly succeeded", value)
		}
	}
}

func TestApprovedPath(t *testing.T) {
	for _, value := range []string{"/opt/tantor/kafka", "/data/kafka/logs", "/usr/lib/jvm/java-17"} {
		if got, err := ApprovedPath(value); err != nil || got != value {
			t.Errorf("ApprovedPath(%q) = %q, %v", value, got, err)
		}
	}
	for _, value := range []string{
		"relative/path", "-rf", "/opt/../../etc", "/tmp/unapproved",
		"/opt/kafka\n/bin/id", "/opt/kafka\x00suffix",
	} {
		if _, err := ApprovedPath(value); err == nil {
			t.Errorf("ApprovedPath(%q) unexpectedly succeeded", value)
		}
	}
}

func TestKafkaServiceNames(t *testing.T) {
	for _, name := range []string{"kafka", "broker", "controller", "zookeeper", "tantor-kafka-1", "tantor-kafka-2.service", "kafka-cluster_a-3"} {
		if _, err := KafkaServiceName(name, "service_name"); err != nil {
			t.Errorf("rejected %q: %v", name, err)
		}
	}
	for _, name := range []string{"--now", "sshd", "tantor-kafka-", "tantor-kafka-../sshd", "tantor-kafka-1;id", "tantor-kafka-1\nExecStart=id", "tantor-kafka-*.service"} {
		if _, err := KafkaServiceName(name, "service_name"); err == nil {
			t.Errorf("accepted invalid name %q", name)
		}
	}
}
