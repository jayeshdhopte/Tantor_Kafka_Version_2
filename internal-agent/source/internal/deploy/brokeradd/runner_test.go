package brokeradd

import (
	"io.translab/tantor-agent/internal/deploy/readiness"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteOnceCannotOverwriteForeignFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(path, []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	if writeOnce(path, "owned", 0600) == nil {
		t.Fatal("foreign content overwritten")
	}
	raw, _ := os.ReadFile(path)
	if string(raw) != "foreign" {
		t.Fatal("foreign content changed")
	}
}
func TestStorageIdentityRequiresAllDirectories(t *testing.T) {
	root := t.TempDir()
	p := readiness.BrokerPlan{KafkaClusterID: "abcdefghijklmnopqrstuv", NodeID: 4, DataPaths: []string{filepath.Join(root, "one"), filepath.Join(root, "two")}}
	if storageState(p) != "MISSING" {
		t.Fatal("fresh storage misclassified")
	}
	os.MkdirAll(p.DataPaths[0], 0755)
	os.WriteFile(filepath.Join(p.DataPaths[0], "meta.properties"), []byte("cluster.id="+p.KafkaClusterID+"\nnode.id=4\ndirectory.id=abcdefghijklmnopqrstuv\n"), 0600)
	if storageState(p) != "PARTIAL" {
		t.Fatal("partial format accepted")
	}
	os.MkdirAll(p.DataPaths[1], 0755)
	os.WriteFile(filepath.Join(p.DataPaths[1], "meta.properties"), []byte("cluster.id=foreign\nnode.id=4\ndirectory.id=two\n"), 0600)
	if storageState(p) != "FOREIGN" {
		t.Fatal("foreign Kafka identity accepted")
	}
}
func TestPropertiesCannotChangeBrokerIdentity(t *testing.T) {
	p := readiness.BrokerPlan{NodeID: 4, HostIP: "192.0.2.20", BrokerPort: 9092, DataPaths: []string{"/data/broker"}, QuorumMode: "DYNAMIC", ControllerEndpoints: []string{"192.0.2.10:9093"}}
	good := p.Properties()
	if err := validateProperties(p, good); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{strings.Replace(good, "roles=broker", "roles=controller", 1), good + "node.id=5\n", strings.Replace(good, "/data/broker", "/data/foreign", 1), good + "controller.quorum.voters=4@192.0.2.20:9093\n"} {
		if validateProperties(p, bad) == nil {
			t.Fatal("unsafe properties accepted")
		}
	}
}
func TestDigestBindsExactFrozenPayload(t *testing.T) {
	if Digest("plan", "config") == Digest("other", "config") || Digest("plan", "config") == Digest("plan", "changed") {
		t.Fatal("payload not bound")
	}
}
func TestRackCannotDifferFromFrozenCandidatePlan(t *testing.T) {
	p := readiness.BrokerPlan{NodeID: 4, HostIP: "192.0.2.20", BrokerPort: 9092, BrokerRack: "rack2", DataPaths: []string{"/data/broker"}, QuorumMode: "DYNAMIC", ControllerEndpoints: []string{"192.0.2.10:9093"}}
	if err := validateProperties(p, p.Properties()); err != nil {
		t.Fatal(err)
	}
	if validateProperties(p, strings.Replace(p.Properties(), "broker.rack=rack2", "broker.rack=rack1", 1)) == nil {
		t.Fatal("rack override accepted")
	}
}
