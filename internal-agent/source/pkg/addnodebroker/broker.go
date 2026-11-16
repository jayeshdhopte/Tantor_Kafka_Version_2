// Package addnodebroker exposes only the operation-owned broker adapter.
package addnodebroker

import (
	"context"
	"encoding/json"
	"fmt"
	"io.translab/tantor-agent/internal/deploy/brokeradd"
	"io.translab/tantor-agent/internal/deploy/readiness"
	"io.translab/tantor-agent/internal/executor"
	"os"
	"runtime"
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
		proof, _ := json.Marshal(map[string]any{"schemaVersion": "add-broker/v1", "operationId": plan.OperationID, "hostId": hostID, "contextHash": plan.ContextHash, "planDigest": params["plan_digest"], "payloadVerified": true, "rootMutationSupported": true, "supportedActions": []string{"PREPARE_ADD_BROKER", "FORMAT_ADD_BROKER", "START_ADD_BROKER", "INSPECT_ADD_BROKER"}})
		return string(proof), nil
	}
	return brokeradd.RunWithDownloader(ctx, command, hostID, stateDir, params, executor.New(executor.Options{PrivilegeMode: "direct"}), download)
}
