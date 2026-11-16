package addnodereadiness

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// Explicit diagnostic on a designated Linux test host. Reads host state and
// binds one temporary test listener; never writes files or runs Kafka tools.
func TestOptInLiveHostInspection(t *testing.T) {
	ip := os.Getenv("TANTOR_EXTERNAL_READINESS_LIVE_IP")
	if ip == "" {
		t.Skip("set designated test-host IP to run live read-only observation")
	}
	now := time.Now().UTC()
	request := map[string]any{
		"schemaVersion": "add-node/v1", "hostId": "phase2-read-only-diagnostic", "hostIp": ip, "hostIdentityUnique": true,
		"contextHash": strings.Repeat("a", 64), "contextStartedAt": now, "expiresAt": now.Add(120 * time.Second),
		"kafkaVersion": "4.3.0", "kafkaClusterId": "diagnostic-only-not-an-admission", "brokerPort": "19092",
		"runtimeUser": "root", "dataPaths": []string{"/opt/tantor-external-phase2-diagnostic/data"},
		"logPath": "/opt/tantor-external-phase2-diagnostic/logs", "installPath": "/opt/tantor-external-phase2-diagnostic/kafka",
		"parcelDir": "/opt/tantor-external-phase2-diagnostic/cache", "artifactChecksum": strings.Repeat("a", 64),
		"ports": []string{"19092"}, "heapXmx": "1G", "limitNofile": 100000,
		"minAvailableMemoryMb": 4096, "minDiskGb": 20, "minInodes": 10000, "allowedJavaMajors": []int{17, 21},
	}
	raw, _ := json.Marshal(request)
	report, err := Run(context.Background(), "INSPECT", string(raw), "phase2-read-only-diagnostic")
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Checks []struct{ Code, Status string }
	}
	if err = json.Unmarshal(report, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Checks) != 20 {
		t.Fatalf("incomplete checks: %d", len(result.Checks))
	}
	for _, check := range result.Checks {
		t.Logf("%s=%s", check.Code, check.Status)
		if check.Code == "C15" && check.Status == "PASS" {
			t.Fatal("missing diagnostic archive falsely passed checksum")
		}
	}
}
