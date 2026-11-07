//go:build linux

package expansion

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/internal/deploy/kafka"
	"io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/pkg/api"
)

// Explicit test-only bridge to the real native adapter. Dedicated QA host IDs,
// units, node IDs and storage namespaces keep production services out of scope.
func TestPhase7IsolatedAdapter(t *testing.T) {
	requestFile := os.Getenv("TANTOR_PHASE7_REQUEST")
	if requestFile == "" {
		t.Skip("isolated acceptance request not supplied")
	}
	if !strings.HasPrefix(requestFile, "/var/tmp/tantor-add-node-phase7-requests/") {
		t.Fatal("request outside isolated QA namespace")
	}
	raw, err := os.ReadFile(requestFile)
	if err != nil {
		t.Fatal(err)
	}
	var request struct {
		Kind, HostID, Base string
		Parameters         map[string]string
		Task               api.Task
	}
	if json.Unmarshal(raw, &request) != nil || !strings.HasPrefix(request.HostID, "phase7-qa-") || !strings.HasPrefix(request.Base, "/var/tmp/tantor-add-node-live-") {
		t.Fatal("invalid isolated QA identity")
	}
	agentState := request.Base + "/agent-state"
	ctx, cancel := context.WithTimeout(context.Background(), 125*time.Second)
	defer cancel()
	e := executor.New(executor.Options{PrivilegeMode: "direct"})
	var output string
	switch request.Kind {
	case "runtime":
		var r RuntimeInspectionRequest
		if json.Unmarshal([]byte(request.Parameters["runtime_inspection_request"]), &r) != nil || !strings.HasPrefix(r.Identity.ServiceName, "tantor-add-node-qa-") || r.Identity.NodeID < 7000 {
			t.Fatal("runtime target is outside isolated QA")
		}
		output, err = InspectRuntime(ctx, request.Parameters["runtime_inspection_request"], request.HostID, e)
	case "candidate", "participant":
		p, decodeErr := Decode(request.Parameters["expansion_plan"], request.Parameters["plan_digest"], request.HostID)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		for _, m := range p.Participants {
			if !strings.HasPrefix(m.Unit, "tantor-add-node-qa-") || !strings.HasPrefix(m.HostID, "phase7-qa-") || m.NodeID < 7000 {
				t.Fatal("participant outside isolated QA")
			}
		}
		for _, c := range p.Processes {
			if c.NodeID < 7000 || c.NodeID > 7999 {
				t.Fatal("candidate ID outside isolated QA")
			}
			for _, path := range append(storagePaths(c), c.LogPath) {
				if !strings.HasPrefix(path, request.Base+"/") {
					t.Fatal("candidate storage outside isolated QA")
				}
			}
		}
		request.Parameters["dispatch_expires_at"] = time.Now().Add(120 * time.Second).UTC().Format(time.RFC3339Nano)
		if request.Kind == "candidate" {
			output, err = RunCandidate(ctx, request.HostID, agentState, request.Parameters, e, nil)
		} else {
			output, err = RunParticipant(ctx, request.HostID, agentState, request.Parameters, e)
		}
	case "cleanup":
		var p struct {
			HostID    string `json:"hostId"`
			Processes []struct {
				NodeID       int      `json:"nodeId"`
				DataPaths    []string `json:"dataPaths"`
				LogPath      string   `json:"logPath"`
				MetadataPath string   `json:"metadataPath"`
			} `json:"processes"`
		}
		if json.Unmarshal([]byte(request.Task.Parameters["add_node_cleanup"]), &p) != nil || p.HostID != request.HostID {
			t.Fatal("cleanup outside QA")
		}
		for _, c := range p.Processes {
			if c.NodeID < 7000 || c.NodeID > 7999 {
				t.Fatal("cleanup ID outside QA")
			}
			for _, path := range append(append([]string{}, c.DataPaths...), c.LogPath, c.MetadataPath) {
				if path != "" && !strings.HasPrefix(path, request.Base+"/") {
					t.Fatal("cleanup storage outside QA")
				}
			}
		}
		cfg := &config.Config{}
		cfg.Agent.HostID = request.HostID
		cfg.Paths.DataDir = agentState
		output, err = kafka.NewDeployer(cfg, nil, e).Clean(ctx, &request.Task)
	case "patch":
		if !strings.HasPrefix(request.Task.Parameters["config_path"], "/etc/tantor-kafka/add-expansion/") || !strings.HasPrefix(request.Task.Parameters["service_name"], "tantor-kafka-broker-7") {
			t.Fatal("configuration target outside QA")
		}
		cfg := &config.Config{}
		cfg.Agent.HostID = request.HostID
		cfg.Paths.DataDir = agentState
		output, err = kafka.NewDeployer(cfg, nil, e).PatchConfig(ctx, &request.Task)
	default:
		t.Fatal("unknown isolated QA action")
	}
	response := map[string]any{"ok": err == nil, "output": output}
	if err != nil {
		response["error"] = err.Error()
	}
	encoded, _ := json.Marshal(response)
	fmt.Printf("TANTOR_PHASE7_RESULT=%s\n", encoded)
	if err != nil {
		t.Fail()
	}
}
