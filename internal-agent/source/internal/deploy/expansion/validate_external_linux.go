//go:build linux

package expansion

import (
	"context"
	"encoding/json"
	"fmt"
	"io.translab/tantor-agent/internal/executor"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ValidateExternalController observes a fresh candidate without creating journal,
// installation, configuration or storage directories. The listener probe is closed.
func ValidateExternalController(ctx context.Context, hostID string, params map[string]string, exec executor.Executor) (string, error) {
	if os.Geteuid() != 0 {
		return "", fmt.Errorf("Linux root required")
	}
	p, err := Decode(params["expansion_plan"], params["plan_digest"], hostID)
	if err != nil {
		return "", err
	}
	if hostID != p.CandidateHostID || p.QuorumMode != "DYNAMIC" || p.Layout != "CONTROLLER" || p.DeploymentSettings == nil {
		return "", fmt.Errorf("external dynamic controller profile required")
	}
	bounded, cancel, err := dispatchContext(ctx, params)
	if err != nil {
		return "", err
	}
	defer cancel()
	env := &nativeCandidateEnvironment{exec: exec}
	if err = env.ValidateArtifact(bounded, p); err != nil {
		return "", err
	}
	if err = env.ValidateFresh(bounded, p, p.Processes[0]); err != nil {
		return "", err
	}
	if err = VerifyExternalControllerMounts(bounded, p, exec); err != nil {
		return "", err
	}
	raw, err := json.Marshal(map[string]any{"schemaVersion": "external-dynamic-controller-ready/v1", "operationId": p.OperationID, "hostId": hostID, "planDigest": params["plan_digest"], "contextHash": p.ContextHash, "candidateReady": true, "payloadVerified": true, "observedAt": time.Now().UTC()})
	return string(raw), err
}

// VerifyExternalControllerMounts applies the supplied C19 mount/access policy to
// the selected controller paths, independently of the unused broker profile.
func VerifyExternalControllerMounts(ctx context.Context, p Plan, e executor.Executor) error {
	c, err := p.Process("controller")
	if err != nil {
		return err
	}
	for _, path := range append(storagePaths(c), c.LogPath) {
		parent := path
		for {
			if err = noLinks(parent); err == nil {
				break
			}
			if !os.IsNotExist(err) {
				return err
			}
			next := filepath.Dir(parent)
			if next == parent {
				return fmt.Errorf("controller storage ancestor unavailable")
			}
			parent = next
		}
		output, _, err := e.Run(ctx, "/usr/bin/findmnt", "-n", "-o", "TARGET,FSTYPE,OPTIONS", "-T", parent)
		if err != nil || !persistentControllerMount(output, path) {
			return fmt.Errorf("controller storage/log path requires a non-root, writable persistent mount: %s", path)
		}
		for _, access := range []string{"-w", "-x"} {
			if _, _, err = e.Run(ctx, "/usr/sbin/runuser", "-u", p.RuntimeUser, "--", "/usr/bin/test", access, parent); err != nil {
				return fmt.Errorf("controller runtime user lacks required mount access: %s", path)
			}
		}
	}
	return nil
}
func persistentControllerMount(output, path string) bool {
	if len(output) > 16384 {
		return false
	}
	fields := strings.Fields(output)
	if len(fields) != 3 || fields[0] == "/" || !(path == fields[0] || strings.HasPrefix(path, fields[0]+"/")) {
		return false
	}
	if fields[1] == "tmpfs" || fields[1] == "ramfs" || fields[1] == "overlay" {
		return false
	}
	for _, option := range strings.Split(fields[2], ",") {
		if option == "ro" {
			return false
		}
	}
	return true
}
