// Package addnodebroker exposes only the operation-owned broker adapter.
package addnodebroker

import (
	"context"
	"encoding/json"
	"fmt"
	"io.translab/tantor-agent/internal/deploy/brokeradd"
	"io.translab/tantor-agent/internal/deploy/layout"
	"io.translab/tantor-agent/internal/deploy/readiness"
	"io.translab/tantor-agent/internal/executor"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

func Run(ctx context.Context, command, hostID, stateDir string, params map[string]string, download func(string, string) (string, error)) (string, error) {
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		return "", fmt.Errorf("broker addition requires Linux root")
	}
	switch command {
	case "VALIDATE_ADD_BROKER_PLAN", "PREPARE_ADD_BROKER", "FORMAT_ADD_BROKER", "START_ADD_BROKER", "INSPECT_ADD_BROKER":
	default:
		return "", fmt.Errorf("unsupported broker-only action")
	}
	if len(params["broker_plan"])+len(params["server_properties"])+len(params["runtime_options"]) > 524288 {
		return "", fmt.Errorf("broker payload exceeds limit")
	}
	plan, err := readiness.ValidateBrokerPlan(params["broker_plan"], hostID)
	if err != nil {
		return "", err
	}
	if plan.SchemaVersion != "add-broker/v3" {
		return "", fmt.Errorf("external broker requires the standard explicit deployment profile")
	}
	if brokeradd.DigestRuntime(params["broker_plan"], params["server_properties"], params["runtime_options"]) != params["plan_digest"] {
		return "", fmt.Errorf("broker payload digest mismatch")
	}
	if command == "VALIDATE_ADD_BROKER_PLAN" {
		if err := readiness.ValidateBrokerPayload(ctx, plan); err != nil {
			return "", err
		}
		proof, _ := json.Marshal(map[string]any{"schemaVersion": "add-broker/v1", "operationId": plan.OperationID, "hostId": hostID, "contextHash": plan.ContextHash, "planDigest": params["plan_digest"], "observedAt": time.Now().UTC(), "payloadVerified": true, "rootMutationSupported": true, "supportedActions": []string{"PREPARE_ADD_BROKER", "FORMAT_ADD_BROKER", "START_ADD_BROKER", "INSPECT_ADD_BROKER"}})
		return string(proof), nil
	}
	proof, err := brokeradd.RunWithDownloader(ctx, command, hostID, stateDir, params, executor.New(executor.Options{PrivilegeMode: "direct"}), download)
	if err != nil || command != "INSPECT_ADD_BROKER" {
		return proof, err
	}
	var report map[string]any
	if json.Unmarshal([]byte(proof), &report) != nil {
		return "", fmt.Errorf("invalid owned receipt")
	}
	complete, active := observeFormatter("/proc", filepath.Join(plan.DeploymentSettings.ConfigDirectory, "broker.properties"))
	report["formatterObservationComplete"] = complete
	report["formatterActive"] = active
	report["storageEmpty"] = brokeradd.EmptyStorage(plan)
	report["runtimeVerified"] = false
	if report["phase"] != "ABSENT" && report["phase"] != "CLAIMED" {
		if err := readiness.ValidateBrokerPayload(ctx, plan); err != nil {
			return "", err
		}
		if err := layout.VerifyRuntime(ctx, filepath.Join(plan.ParcelDir, "kafka", plan.KafkaVersion), plan.InstallPath); err != nil {
			return "", err
		}
		report["runtimeVerified"] = true
	}
	report["processVerified"] = false
	if report["serviceActive"] == true {
		exec := executor.New(executor.Options{PrivilegeMode: "direct"})
		pid, _, err := exec.Run(ctx, "systemctl", "show", "--property=MainPID", "--value", plan.ServiceName()+".service")
		if err != nil {
			return "", err
		}
		if err := verifyBrokerProcess("/proc", pid, filepath.Join(plan.DeploymentSettings.ConfigDirectory, "broker.properties"), plan.JavaHome, plan.InstallPath, plan.HeapXmx); err != nil {
			return "", err
		}
		after, _, err := exec.Run(ctx, "systemctl", "show", "--property=MainPID", "--value", plan.ServiceName()+".service")
		if err != nil || strings.TrimSpace(pid) != strings.TrimSpace(after) {
			return "", fmt.Errorf("broker process changed during inspection")
		}
		report["processVerified"] = true
	}
	report["observedAt"] = time.Now().UTC()
	encoded, err := json.Marshal(report)
	return string(encoded), err
}
