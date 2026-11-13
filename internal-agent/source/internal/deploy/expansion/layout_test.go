package expansion

import (
	"io.translab/tantor-agent/internal/deploy/layout"
	"strings"
	"testing"
)

func TestStandardLayoutUsesTwoRoleServicesAndRejectsProfileDrift(t *testing.T) {
	p := fixture()
	c := p.Processes[0]
	b := p.Processes[1]
	p.SchemaVersion = "add-quorum-expansion/v2"
	p.DeploymentSettings = &layout.Settings{InstallPath: "/opt/tantor/kafka", ConfigDirectory: "/opt/tantor/kafka/config", JavaHome: p.JavaHome, RuntimeUser: p.RuntimeUser, BrokerDataPaths: b.DataPaths, BrokerMetadataPath: b.MetadataPath, BrokerLogPath: b.LogPath, BrokerHeapXmx: b.HeapXmx, ControllerDataPaths: c.DataPaths, ControllerMetadataPath: c.MetadataPath, ControllerLogPath: c.LogPath, ControllerHeapXmx: c.HeapXmx}
	if e := p.Validate(); e != nil {
		t.Fatal(e)
	}
	if p.Unit("broker") != "broker.service" || p.Unit("controller") != "controller.service" || p.ConfigPath("broker") != "/opt/tantor/kafka/config/broker.properties" {
		t.Fatal("nonstandard candidate layout")
	}
	props, e := p.Properties("broker")
	if e != nil || !strings.Contains(props, "process.roles=broker\n") {
		t.Fatal("role-only configuration", e)
	}
	p.Processes[1].HeapXmx = "2G"
	if e := p.Validate(); e == nil {
		t.Fatal("profile drift accepted")
	}
}
