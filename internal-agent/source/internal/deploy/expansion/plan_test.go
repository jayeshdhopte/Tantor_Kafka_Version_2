package expansion

import (
	"encoding/json"
	"strings"
	"testing"
)

func fixture() Plan {
	uuid1 := "12345678-1234-1234-1234-123456789abc"
	uuid2 := "12345678-1234-1234-1234-123456789abd"
	member := func(id int, role, host, ip string) Participant {
		return Participant{ServiceIDs: []string{uuid1}, HostID: host, HostIP: ip, Role: role, NodeID: id, Unit: role + ".service", ConfigPath: "/opt/kafka/config/" + role + ".properties", ConfigSHA256: strings.Repeat("a", 64), UnitSHA256: strings.Repeat("b", 64), ArtifactProofSHA256: strings.Repeat("c", 64), RuntimeHome: "/opt/kafka", Storage: []Storage{{Path: "/data/" + role, DirectoryID: strings.Repeat("d", 22)}}, PID: 42, StartTicks: "123456", ControllerPort: 9093}
	}
	broker := member(1, "broker", "existing-broker", "192.0.2.1")
	controller := member(101, "controller", "existing-controller", "192.0.2.2")
	controller.ServiceIDs = []string{uuid2}
	broker.Storage[0].DirectoryID = strings.Repeat("f", 22)
	process := func(role string, id, port int) Process {
		return Process{Role: role, NodeID: id, Port: port, DataPaths: []string{"/data/new-" + role + "/data"}, LogPath: "/var/log/new-" + role, HeapXmx: "512m", Runtime: Runtime{LimitNofile: 100000}, Inherited: map[string]string{"num.network.threads": "3"}}
	}
	newController := process("controller", 102, 9093)
	newController.MetadataPath = "/data/new-controller/metadata"
	return Plan{ResourcePolicy: ResourcePolicy{MinAvailableMemoryMB: 4096, OSReserveMB: 2048, MinDiskGB: 20, MinInodes: 10000, AllowedJavaMajors: []int{17, 21}}, SchemaVersion: "add-quorum-expansion/v1", OperationID: uuid1, ClusterID: uuid2, ContextHash: strings.Repeat("a", 64), CandidateHostID: "candidate", CandidateIP: "192.0.2.3", KafkaClusterID: strings.Repeat("a", 22), KafkaVersion: "4.3.0", QuorumMode: "DYNAMIC", Layout: "SEPARATE", Features: map[string]int16{"metadata.version": 25, "kraft.version": 1}, ArtifactID: uuid1, ArtifactSHA256: strings.Repeat("e", 64), ParcelDir: "/opt/tantor/parcels", JavaHome: "/opt/java/jdk21", RuntimeUser: "kafka", Processes: []Process{newController, process("broker", 2, 9092)}, Participants: []Participant{broker, controller}, Voters: []Voter{{NodeID: 101, DirectoryID: strings.Repeat("d", 22), HostIP: "192.0.2.2", Port: 9093}}, ExcludedHostIDs: []string{"broker6"}, ExcludedHostIPs: []string{"192.168.3.213"}}
}
func TestDynamicPropertiesAreTwoRoleOnlyProcessesWithIndependentFiles(t *testing.T) {
	p := fixture()
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"controller", "broker"} {
		text, err := p.Properties(role)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(text, "process.roles="+role+"\n") || strings.Contains(text, "controller.quorum.voters=") || !strings.Contains(text, "controller.quorum.bootstrap.servers=192.0.2.2:9093") {
			t.Fatal(text)
		}
	}
	if p.ConfigPath("controller") == p.ConfigPath("broker") || p.Unit("controller") == p.Unit("broker") {
		t.Fatal("process namespace shared")
	}
}
func TestStaticVotersAreIdenticalAndContainOnlyTheNewController(t *testing.T) {
	p := fixture()
	p.QuorumMode = "STATIC"
	p.Features["kraft.version"] = 0
	p.MaintenanceAcknowledged = true
	for _, role := range []string{"controller", "broker"} {
		text, err := p.Properties(role)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(text, "controller.quorum.voters="+p.ExpandedVoters()+"\n") || strings.Contains(text, "controller.quorum.bootstrap.servers=") {
			t.Fatal(text)
		}
	}
	if p.ExpandedVoters() != "101@192.0.2.2:9093,102@192.0.2.3:9093" {
		t.Fatal("unexpected voter set: " + p.ExpandedVoters())
	}
}
func TestQuorumModeCannotDefaultOrMigrateImplicitly(t *testing.T) {
	for _, change := range []func(*Plan){func(p *Plan) { p.QuorumMode = "UNKNOWN" }, func(p *Plan) { p.Features["kraft.version"] = 0 }, func(p *Plan) { p.QuorumMode = "STATIC"; p.MaintenanceAcknowledged = true }, func(p *Plan) { p.QuorumMode = "STATIC"; p.Features["kraft.version"] = 0 }} {
		p := fixture()
		change(&p)
		if p.Validate() == nil {
			t.Fatal("unsafe quorum branch accepted")
		}
	}
}
func TestTwoIdentityReservationCannotSkipCollideOrAliasAHost(t *testing.T) {
	for _, change := range []func(*Plan){func(p *Plan) { p.Processes[1].NodeID = 3 }, func(p *Plan) { p.Processes[1].NodeID = 102 }, func(p *Plan) { p.Processes[0].NodeID = 1 }, func(p *Plan) { p.CandidateIP = p.Participants[0].HostIP }} {
		p := fixture()
		change(&p)
		if p.Validate() == nil {
			t.Fatal("unsafe candidate identity accepted")
		}
	}
}
func TestExcludedHostsAndIpsAreRejectedForEveryLayout(t *testing.T) {
	for _, change := range []func(*Plan){func(p *Plan) { p.CandidateHostID = "broker6" }, func(p *Plan) { p.CandidateIP = "192.168.3.213" }, func(p *Plan) { p.Participants[0].HostIP = "192.168.3.213" }} {
		p := fixture()
		change(&p)
		if p.Validate() == nil {
			t.Fatal("excluded node accepted")
		}
	}
}
func TestMutableDirectoriesAndPortsCannotOverlap(t *testing.T) {
	for _, change := range []func(*Plan){func(p *Plan) { p.Processes[1].DataPaths = p.Processes[0].DataPaths }, func(p *Plan) { p.Processes[1].LogPath = p.Processes[0].LogPath }, func(p *Plan) { p.Processes[1].Port = p.Processes[0].Port }, func(p *Plan) { p.Processes[0].Runtime.JmxPort = p.Processes[1].Port }, func(p *Plan) { p.Processes[1].LogPath = p.ParcelDir + "/logs" }} {
		p := fixture()
		change(&p)
		if p.Validate() == nil {
			t.Fatal("overlapping process resources accepted")
		}
	}
}
func TestInheritedPropertiesCannotReplaceIdentityOrInstallSecretAdapters(t *testing.T) {
	for _, key := range []string{"node.id", "process.roles", "cluster.id", "sasl.jaas.config", "ssl.keystore.location", "security.protocol"} {
		p := fixture()
		p.Processes[1].Inherited[key] = "value"
		if p.Validate() == nil {
			t.Fatal("unsupported inherited key accepted: " + key)
		}
	}
}
func TestCopiedDirectoryIdentitiesAndUnboundedHeapsAreRejected(t *testing.T) {
	for _, change := range []func(*Plan){func(p *Plan) { p.Participants[0].Storage[0].DirectoryID = p.Participants[1].Storage[0].DirectoryID }, func(p *Plan) { p.Voters[0].DirectoryID = "AAAAAAAAAAAAAAAAAAAAAA" }, func(p *Plan) { p.Voters[0].DirectoryID = strings.Repeat("g", 22) }, func(p *Plan) { p.Processes[0].HeapXmx = "99999999999999999999g" }} {
		p := fixture()
		change(&p)
		if p.Validate() == nil {
			t.Fatal("unsafe directory or memory accounting accepted")
		}
	}
	if n, err := HeapBytes("2g"); err != nil || n != 2*1024*1024*1024 {
		t.Fatal(n, err)
	}
}
func TestDigestAndAgentHostBindTheExactFrozenParent(t *testing.T) {
	p := fixture()
	raw, _ := json.Marshal(p)
	if _, err := Decode(string(raw), Digest(string(raw)), "candidate"); err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(string(raw), strings.Repeat("f", 64), "candidate"); err == nil {
		t.Fatal("wrong digest accepted")
	}
	if _, err := Decode(string(raw), Digest(string(raw)), "other-host"); err == nil {
		t.Fatal("foreign host accepted")
	}
}
func TestControllerOnlyRetainsTheSameIdentityAndScopeRules(t *testing.T) {
	p := fixture()
	p.Layout = "CONTROLLER"
	p.Processes = p.Processes[:1]
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Process("broker"); err == nil {
		t.Fatal("broker process invented")
	}
}
