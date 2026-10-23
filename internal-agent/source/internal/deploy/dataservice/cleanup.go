package dataservice

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/pkg/api"
	"io.translab/tantor-agent/pkg/logrotation"
)

// Clean removes only an installed Tantor-managed data service. An absent unit
// is normal after a failed precheck or a partially completed cleanup retry.
func Clean(ctx context.Context, exec executor.Executor, kind string, task *api.Task) (string, error) {
	s, err := Parse(kind, task)
	if err != nil {
		return "", err
	}
	if kind == Connect {
		connectDeploymentLock.Lock()
		defer connectDeploymentLock.Unlock()
	} else {
		schemaDeploymentLock.Lock()
		defer schemaDeploymentLock.Unlock()
	}
	unitPath := "/etc/systemd/system/" + s.ServiceName + ".service"
	_, _, unitErr := exec.RunSudo(ctx, "test", "-f", unitPath)
	installed := unitErr == nil
	if !installed {
		marker, _, markerErr := exec.RunSudo(ctx, "cat", "--", ownershipPath(s))
		if markerErr == nil && strings.TrimSpace(marker) == ownershipContent(s) {
			// Installation can fail after copying files but before writing a unit.
			// Only a matching local ownership marker permits removing those files.
			installed = false
		} else {
			if markerErr == nil && strings.TrimSpace(marker) != "" {
				return "", fmt.Errorf("service ownership record differs; refusing to infer cleanup paths")
			}
			if err := logrotation.RemoveApplicationRetention(ctx, exec, s.Log, s.ServiceName+".log", "tantor-"+s.ServiceName); err != nil {
				return "", err
			}
			// Old partial installs may predate ownership records. Keep the failed
			// deletion visible rather than claiming success with files left behind.
			if _, detail, err := exec.RunSudo(ctx, "bash", append([]string{"-c", `for dir in "$@"; do if [ -e "$dir" ] || [ -L "$dir" ]; then echo "Unowned service path remains: $dir" >&2; exit 1; fi; done`, "--"}, cleanupDirectories(s)...)...); err != nil {
				return "", fmt.Errorf("service unit/ownership record is missing; inspect remaining paths before retrying: %s", detail)
			}
			return "[SKIP] " + kind + " unit is not installed\n", nil
		}
	}
	if installed {
		unit, stderr, err := exec.RunSudo(ctx, "cat", unitPath)
		if err != nil {
			return "", fmt.Errorf("read %s: %w (%s)", unitPath, err, stderr)
		}
		launcher := "schema-registry-start"
		configFile := "schema-registry.properties"
		if kind == Connect {
			launcher = "connect-distributed.sh"
			configFile = "connect-distributed.properties"
		}
		expectedStart := "ExecStart=" + s.Install + "/bin/" + launcher + " " + s.Config + "/" + configFile
		if !strings.Contains(unit, "Description=Tantor "+kind+"\n") || !strings.Contains(unit, expectedStart+"\n") {
			return "", fmt.Errorf("refusing to remove %s: unit does not match the managed service and paths", unitPath)
		}
	}
	if err := CheckPaths(ctx, exec, s); err != nil {
		return "", err
	}
	for _, dir := range cleanupDirectories(s) {
		resolved, stderr, err := exec.Run(ctx, "readlink", "-m", "--", dir)
		if err != nil || strings.TrimSpace(resolved) != dir {
			return "", fmt.Errorf("refusing to remove redirected service path %s (%s): %v", dir, stderr, err)
		}
	}
	unitName := s.ServiceName + ".service"
	if err := logrotation.RemoveApplicationRetention(ctx, exec, s.Log, s.ServiceName+".log", "tantor-"+s.ServiceName); err != nil {
		return "", err
	}
	manageUnit := installed
	if !manageUnit {
		state, detail, err := exec.RunSudo(ctx, "systemctl", "show", unitName, "--property=LoadState", "--value")
		if err != nil {
			return "", fmt.Errorf("inspect partially installed unit: %w (%s)", err, detail)
		}
		manageUnit = strings.TrimSpace(state) != "not-found"
	}
	if manageUnit {
		for _, action := range []string{"stop", "disable"} {
			if out, stderr, err := exec.RunSudo(ctx, "systemctl", action, unitName); err != nil {
				return "", fmt.Errorf("%s %s: %w (%s %s)", action, unitName, err, out, stderr)
			}
		}
	}
	for _, dir := range cleanupDirectories(s) {
		if out, stderr, err := exec.RunSudo(ctx, "rm", "-rf", "--", dir); err != nil {
			return "", fmt.Errorf("remove %s: %w (%s %s)", dir, err, out, stderr)
		}
	}
	if out, stderr, err := exec.RunSudo(ctx, "rm", "-rf", "--", unitPath+".d"); err != nil {
		return "", fmt.Errorf("remove service overrides: %w (%s %s)", err, out, stderr)
	}
	if out, stderr, err := exec.RunSudo(ctx, "rm", "-f", "--", unitPath); err != nil {
		return "", fmt.Errorf("remove %s: %w (%s %s)", unitPath, err, out, stderr)
	}
	if out, stderr, err := exec.RunSudo(ctx, "systemctl", "daemon-reload"); err != nil {
		return "", fmt.Errorf("reload systemd: %w (%s %s)", err, out, stderr)
	}
	return "[PASS] Removed managed " + kind + " service and directories\n", nil
}

func ownershipPath(s Settings) string {
	return s.Install + "/.tantor-" + s.ServiceName + "-ownership.json"
}
func ownershipContent(s Settings) string {
	data, _ := json.Marshal(struct {
		Kind        string   `json:"kind"`
		Service     string   `json:"service"`
		Directories []string `json:"directories"`
	}{s.Kind, s.ServiceName, s.Directories()})
	return string(data)
}

func cleanupDirectories(s Settings) []string {
	// Shared runtime/config/plugins belong to Kafka. The service's separate
	// log/working directories are still deployment-owned and must be removed.
	if s.SharedConnect || s.SharedSchema {
		return []string{s.Log, s.Working}
	}
	return s.Directories()
}
