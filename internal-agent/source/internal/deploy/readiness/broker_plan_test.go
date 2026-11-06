package readiness

import (
	"encoding/json"
	"strings"
	"testing"
)

func brokerFixture() BrokerPlan {
	return BrokerPlan{SchemaVersion: "add-broker/v1", OperationID: "12345678-1234-1234-1234-123456789abc", HostID: "candidate", ContextHash: strings.Repeat("a", 64), KafkaClusterID: "abcdefghijklmnopqrstuv", NodeID: 4, HostIP: "192.168.3.195", BrokerPort: 9092, KafkaVersion: "4.1.2", RuntimeUser: "kafka", JavaHome: "/opt/java/jdk21", ParcelDir: "/opt/tantor/parcels", ArtifactChecksum: strings.Repeat("b", 64), DataPaths: []string{"/data/kafka/broker"}, MetadataPath: "/data/kafka/metadata", LogPath: "/var/log/kafka/broker", InstallPath: "/opt/tantor/services/kafka", HeapXmx: "1g", QuorumMode: "STATIC", ControllerEndpoints: []string{"192.168.3.194:9093"}, QuorumVoters: "1@192.168.3.194:9093"}
}
func TestBrokerConfigFilenameIsVersionedForSafeResume(t *testing.T) {
	for schema, file := range map[string]string{"add-broker/v1": "server.properties", "add-broker/v2": "broker.properties"} {
		p := brokerFixture()
		p.SchemaVersion = schema
		raw, _ := json.Marshal(p)
		parsed, err := ValidateBrokerPlan(string(raw), "candidate")
		if err != nil {
			t.Fatal(err)
		}
		if parsed.ConfigFileName() != file {
			t.Fatal("frozen configuration filename changed")
		}
	}
}
func TestBrokerPlanRejectsIdentityVoterAndPathConfusion(t *testing.T) {
	cases := []func(*BrokerPlan){func(p *BrokerPlan) { p.HostID = "another-host" }, func(p *BrokerPlan) { p.NodeID = 1 }, func(p *BrokerPlan) { p.DataPaths = []string{"/data/../etc"} }, func(p *BrokerPlan) { p.DataPaths = []string{"/opt/tantor"} }, func(p *BrokerPlan) { p.RuntimeUser = "kafka;id" }, func(p *BrokerPlan) { p.QuorumMode = "UNKNOWN" }, func(p *BrokerPlan) { p.ControllerEndpoints = []string{"host:9093\nnode.id=1"} }}
	for i, change := range cases {
		p := brokerFixture()
		change(&p)
		raw, _ := json.Marshal(p)
		if _, err := ValidateBrokerPlan(string(raw), "candidate"); err == nil {
			t.Fatalf("unsafe case %d accepted", i)
		}
	}
}
func TestBrokerPlanPropertiesNeverCreateController(t *testing.T) {
	for _, mode := range []string{"STATIC", "DYNAMIC"} {
		p := brokerFixture()
		p.QuorumMode = mode
		if mode == "DYNAMIC" {
			p.QuorumVoters = ""
		}
		raw, _ := json.Marshal(p)
		validated, err := ValidateBrokerPlan(string(raw), "candidate")
		if err != nil {
			t.Fatal(err)
		}
		config := validated.Properties()
		if !strings.Contains(config, "process.roles=broker\n") || strings.Contains(config, "process.roles=controller") || strings.Contains(config, "initial.controllers") {
			t.Fatal("broker configuration changed controller membership")
		}
		if mode == "DYNAMIC" && strings.Contains(config, "controller.quorum.voters=") {
			t.Fatal("static voters added to dynamic broker")
		}
	}
}
func TestBrokerPlanPreservesFourThreeVersion(t *testing.T) {
	p := brokerFixture()
	p.KafkaVersion = "4.3.0"
	raw, _ := json.Marshal(p)
	validated, err := ValidateBrokerPlan(string(raw), "candidate")
	if err != nil {
		t.Fatal(err)
	}
	if validated.KafkaVersion != "4.3.0" || validated.KafkaClusterID != p.KafkaClusterID || validated.NodeID != p.NodeID {
		t.Fatal("broker plan changed inherited identity/version")
	}
}
