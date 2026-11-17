// Package addnodecontroller exposes scoped external dynamic controller and separate-process workflows.
package addnodecontroller

import (
	"context"
	"encoding/json"
	"fmt"
	"io.translab/tantor-agent/internal/deploy/expansion"
	"io.translab/tantor-agent/internal/deploy/layout"
	"io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/pkg/addnodebroker"
	"os"
	"runtime"
	"time"
)

func Run(ctx context.Context, action, hostID, stateDir string, params map[string]string, execution bool, download func(string, string) (string, error)) (string, error) {
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		return "", fmt.Errorf("Linux root required")
	}
	return run(ctx, action, hostID, stateDir, params, execution, false, download)
}

func RunSeparate(ctx context.Context, action, hostID, stateDir string, params map[string]string, execution bool, download func(string, string) (string, error)) (string, error) {
	return run(ctx, action, hostID, stateDir, params, false, execution, download)
}

func run(ctx context.Context, action, hostID, stateDir string, params map[string]string, execution, separateExecution bool, download func(string, string) (string, error)) (string, error) {
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		return "", fmt.Errorf("Linux root required")
	}
	exec := executor.New(executor.Options{PrivilegeMode: "direct"})
	if action == "RUNTIME" {
		return expansion.InspectRuntime(ctx, params["runtime_inspection_request"], hostID, exec)
	}
	p, err := expansion.Decode(params["expansion_plan"], params["plan_digest"], hostID)
	if err != nil {
		return "", err
	}
	if hostID != p.CandidateHostID || p.QuorumMode != "DYNAMIC" || (p.Layout != "CONTROLLER" && p.Layout != "SEPARATE") || p.DeploymentSettings == nil {
		return "", fmt.Errorf("only a reviewed external dynamic controller is supported")
	}
	if action == "VALIDATE" {
		return expansion.ValidateExternalController(ctx, hostID, params, exec)
	}
	if !(p.Layout == "SEPARATE" && separateExecution || p.Layout == "CONTROLLER" && execution) {
		return "", fmt.Errorf("controller execution requires its separate opt-in")
	}
	switch action {
	case "INSPECT", "PREPARE", "FORMAT", "START", "ADMIT", "RECONCILE_FORMAT":
	default:
		return "", fmt.Errorf("unsupported external controller action")
	}
	role := params["role"]
	if (role != "controller" && !(p.Layout == "SEPARATE" && role == "broker")) || action == "ADMIT" && role != "controller" || params["action"] != action {
		return "", fmt.Errorf("controller action/role mismatch")
	}
	if action != "INSPECT" {
		if err := expansion.VerifyExternalControllerMounts(ctx, p, exec); err != nil {
			return "", err
		}
	}
	raw, err := expansion.RunCandidate(ctx, hostID, stateDir, params, exec, download)
	if err != nil || action != "INSPECT" {
		return raw, err
	}
	var report map[string]any
	if json.Unmarshal([]byte(raw), &report) != nil {
		return "", fmt.Errorf("invalid owned controller inspection")
	}
	observation, ok := report["observation"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("missing controller observation")
	}
	complete, active := addnodebroker.ObserveFormatter(p.ConfigPath(role))
	observation["formatterObservationComplete"] = complete
	observation["formatterActive"] = active
	admissionComplete, admissionActive := addnodebroker.ObserveControllerAdmission(p.ConfigPath(role))
	observation["admissionObservationComplete"] = admissionComplete
	observation["admissionActive"] = admissionActive
	process, err := p.Process(role)
	if err != nil {
		return "", err
	}
	observation["storageEmpty"] = storageEmpty(process)
	observation["mountsVerified"] = expansion.VerifyExternalControllerMounts(ctx, p, exec) == nil
	report["observedAt"] = time.Now().UTC()
	encoded, err := json.Marshal(report)
	return string(encoded), err
}

func storageEmpty(process expansion.Process) bool {
	paths := append([]string{}, process.DataPaths...)
	if process.MetadataPath != "" {
		paths = append(paths, process.MetadataPath)
	}
	empty := true
	for _, path := range paths {
		if layout.CheckDirectory(path) != nil {
			empty = false
			continue
		}
		entries, e := os.ReadDir(path)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil || len(entries) > 0 {
			empty = false
		}
	}
	return empty
}
