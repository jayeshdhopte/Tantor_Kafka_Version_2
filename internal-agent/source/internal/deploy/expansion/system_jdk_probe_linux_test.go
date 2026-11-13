//go:build linux

package expansion

import (
	"context"
	"io.translab/tantor-agent/internal/executor"
	"os"
	"strings"
	"testing"
	"time"
)

// Opt-in, read-only probe: no unit, receipt, directory or Kafka membership mutation.
func TestSystemJDKReadOnlyCandidateAdmission(t *testing.T) {
	if os.Getenv("TANTOR_SYSTEM_JDK_PROBE") != "191" {
		t.Skip("explicit read-only 191 JDK probe not requested")
	}
	if os.Geteuid() != 0 {
		t.Fatal("root probe required")
	}
	p := fixture()
	p.JavaHome = "/usr/lib/jvm/jdk-17"
	p.CandidateIP = "192.168.3.191"
	p.Processes[0].DataPaths = []string{"/opt/tantor-controller-storage/data"}
	p.Processes[0].MetadataPath = "/opt/tantor-controller-storage/metadata"
	p.Processes[0].LogPath = "/opt/tantor-controller-storage/logs"
	p.Processes[1].DataPaths = []string{"/opt/tantor-broker-storage/data"}
	p.Processes[1].LogPath = "/opt/tantor-broker-storage/logs"
	p.Processes[0].HeapXmx = "2g"
	p.Processes[1].HeapXmx = "1g"
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	e := &nativeCandidateEnvironment{exec: executor.New(executor.Options{PrivilegeMode: "direct"})}
	err := e.ValidateFresh(ctx, p, p.Processes[0])
	if err == nil || !strings.Contains(err.Error(), "shared filesystem cannot cover all process reservations") {
		t.Fatalf("expected real capacity check after system Java admission, got: %v", err)
	}
	t.Log("System Java admitted; unchanged production capacity policy correctly reports:", err)
}
