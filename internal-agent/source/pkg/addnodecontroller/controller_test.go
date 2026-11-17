package addnodecontroller

import (
	"context"
	"encoding/json"
	"io.translab/tantor-agent/internal/deploy/expansion"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func validExternalPlan(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "internal", "deploy", "expansion", "testdata", "separate-dynamic-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var plan map[string]any
	if json.Unmarshal(raw, &plan) != nil {
		t.Fatal("invalid fixture")
	}
	processes := plan["processes"].([]any)
	controller := processes[0].(map[string]any)
	broker := processes[1].(map[string]any)
	plan["schemaVersion"] = "add-quorum-expansion/v2"
	plan["layout"] = "CONTROLLER"
	plan["processes"] = processes[:1]
	plan["deploymentSettings"] = map[string]any{"installPath": "/opt/tantor/kafka", "configDirectory": "/opt/tantor/kafka/config", "javaHome": plan["javaHome"], "runtimeUser": plan["runtimeUser"], "controllerDataPaths": controller["dataPaths"], "controllerMetadataPath": controller["metadataPath"], "controllerLogPath": controller["logPath"], "controllerHeapXmx": controller["heapXmx"], "brokerDataPaths": broker["dataPaths"], "brokerMetadataPath": broker["metadataPath"], "brokerLogPath": broker["logPath"], "brokerHeapXmx": broker["heapXmx"]}
	bytes, _ := json.Marshal(plan)
	if _, err := expansion.Decode(string(bytes), expansion.Digest(string(bytes)), "candidate"); err != nil {
		t.Fatal(err)
	}
	return plan
}
func TestExternalFacadeRejectsStaticSeparateAndUnapprovedActions(t *testing.T) {
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		t.Skip("Linux root adapter")
	}
	for _, change := range []string{"no-opt-in", "static", "missing-settings", "unsupported"} {
		plan := validExternalPlan(t)
		action := "PREPARE"
		execution := change != "no-opt-in"
		if change == "static" {
			plan["quorumMode"] = "STATIC"
			plan["maintenanceAcknowledged"] = true
			plan["features"].(map[string]any)["kraft.version"] = float64(0)
		}
		if change == "missing-settings" {
			plan["schemaVersion"] = "add-quorum-expansion/v1"
			delete(plan, "deploymentSettings")
		}
		if change == "unsupported" {
			action = "STOP"
		}
		bytes, _ := json.Marshal(plan)
		params := map[string]string{"expansion_plan": string(bytes), "plan_digest": expansion.Digest(string(bytes)), "role": "controller", "action": action}
		calls := 0
		state := t.TempDir()
		_, err := Run(context.Background(), action, "candidate", state, params, execution, func(string, string) (string, error) { calls++; return "", nil })
		if err == nil || calls != 0 {
			t.Fatalf("%s escaped external scope/opt-in", change)
		}
		entries, _ := os.ReadDir(state)
		if len(entries) != 0 {
			t.Fatal("rejected scope created machine state")
		}
	}
}
func TestFailedPreviewDoesNotCreateOperationJournal(t *testing.T) {
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		t.Skip("Linux root adapter")
	}
	plan := validExternalPlan(t)
	plan["parcelDir"] = filepath.Join("/var/tmp", filepath.Base(t.TempDir()), "missing-parcels")
	raw, _ := json.Marshal(plan)
	params := map[string]string{"expansion_plan": string(raw), "plan_digest": expansion.Digest(string(raw)), "role": "controller", "action": "VALIDATE", "dispatch_expires_at": time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano)}
	state := t.TempDir()
	if _, err := Run(context.Background(), "VALIDATE", "candidate", state, params, false, nil); err == nil {
		t.Fatal("missing approved cached package accepted")
	}
	entries, _ := os.ReadDir(state)
	if len(entries) != 0 {
		t.Fatal("preview wrote its operation journal")
	}
}

func TestSeparateFacadeNeedsExplicitOptInAndRejectsParticipantActions(t *testing.T) {
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		t.Skip("Linux root adapter")
	}
	p := validExternalPlan(t)
	source, err := os.ReadFile(filepath.Join("..", "..", "internal", "deploy", "expansion", "testdata", "separate-dynamic-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var original map[string]any
	json.Unmarshal(source, &original)
	p["processes"] = original["processes"]
	p["layout"] = "SEPARATE"
	raw, _ := json.Marshal(p)
	params := map[string]string{"expansion_plan": string(raw), "plan_digest": expansion.Digest(string(raw)), "role": "broker", "action": "PREPARE"}
	if _, err := expansion.Decode(string(raw), params["plan_digest"], "candidate"); err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	if _, err := Run(context.Background(), "PREPARE", "candidate", state, params, true, nil); err == nil {
		t.Fatal("controller opt-in authorized separate processes")
	}
	if _, err := RunSeparate(context.Background(), "PREPARE", "candidate", state, params, false, nil); err == nil {
		t.Fatal("separate default authorized mutation")
	}
	params["action"] = "STOP"
	if _, err := RunSeparate(context.Background(), "STOP", "candidate", state, params, true, nil); err == nil {
		t.Fatal("participant stop escaped scope")
	}
	entries, _ := os.ReadDir(state)
	if len(entries) != 0 {
		t.Fatal("rejected scope created candidate state")
	}
}

func TestBrokerEmptyMetadataSettingUsesOnlyDataDirectoriesForRecovery(t *testing.T) {
	path := t.TempDir()
	p := expansion.Process{Role: "broker", DataPaths: []string{path}, MetadataPath: ""}
	if !storageEmpty(p) {
		t.Fatal("empty metadata setting was treated as a foreign storage path")
	}
	if err := os.WriteFile(filepath.Join(path, "meta.properties"), []byte("formatted"), 0600); err != nil {
		t.Fatal(err)
	}
	if storageEmpty(p) {
		t.Fatal("formatted data mistaken for empty storage")
	}
}
