//go:build linux

package expansion

import (
	"context"
	"encoding/json"
	"io.translab/tantor-agent/internal/executor"
	"os"
	"testing"
)

// Explicit opt-in, read-only check of a named live service. No service changes.
func TestApprovedLiveRuntimeInspection(t *testing.T) {
	name := os.Getenv("TANTOR_QUORUM_RUNTIME_REQUEST")
	if name == "" {
		t.Skip("explicit read-only live runtime request not supplied")
	}
	raw, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var request RuntimeInspectionRequest
	if err = json.Unmarshal(raw, &request); err != nil {
		t.Fatal(err)
	}
	reportRaw, err := InspectRuntime(context.Background(), string(raw), request.Identity.HostID, executor.New(executor.Options{PrivilegeMode: "direct"}))
	if err != nil {
		t.Fatal(err)
	}
	var report RuntimeInspectionReport
	if err = json.Unmarshal([]byte(reportRaw), &report); err != nil {
		t.Fatal(err)
	}
	if !report.Identity.IdentityVerified || report.Runtime.KafkaVersion != request.Identity.KafkaVersion || report.Runtime.ArchiveSHA256 != request.ArtifactSHA256 {
		t.Fatal("live identity/version proof missing")
	}
	t.Logf("Verified live node %d, Kafka %s, %d classpath libraries and %d canonical file bindings", report.Identity.NodeID, report.Runtime.KafkaVersion, report.Runtime.LibraryCount, len(report.ResolvedPaths))
}
