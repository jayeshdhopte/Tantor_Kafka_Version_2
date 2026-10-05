package dataservice

import (
	"context"
	"fmt"
	"strings"

	"io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/pkg/api"
)

// Clean removes only an installed Tantor-managed data service. An absent unit
// is normal after a failed precheck or a partially completed cleanup retry.
func Clean(ctx context.Context, exec executor.Executor, kind string, task *api.Task) (string, error) {
	s, err := Parse(kind, task)
	if err != nil {
		return "", err
	}
	unitPath := "/etc/systemd/system/" + kind + ".service"
	if _, _, err := exec.RunSudo(ctx, "test", "-f", unitPath); err != nil {
		return "[SKIP] " + kind + " unit is not installed\n", nil
	}
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
	if err := CheckPaths(ctx, exec, s); err != nil {
		return "", err
	}
	for _, dir := range s.Directories() {
		resolved, stderr, err := exec.Run(ctx, "readlink", "-m", "--", dir)
		if err != nil || strings.TrimSpace(resolved) != dir {
			return "", fmt.Errorf("refusing to remove redirected service path %s (%s): %v", dir, stderr, err)
		}
	}
	unitName := kind + ".service"
	for _, action := range []string{"stop", "disable"} {
		if out, stderr, err := exec.RunSudo(ctx, "systemctl", action, unitName); err != nil {
			return "", fmt.Errorf("%s %s: %w (%s %s)", action, unitName, err, out, stderr)
		}
	}
	for _, dir := range s.Directories() {
		if out, stderr, err := exec.RunSudo(ctx, "rm", "-rf", "--", dir); err != nil {
			return "", fmt.Errorf("remove %s: %w (%s %s)", dir, err, out, stderr)
		}
	}
	if out, stderr, err := exec.RunSudo(ctx, "rm", "-f", "--", unitPath); err != nil {
		return "", fmt.Errorf("remove %s: %w (%s %s)", unitPath, err, out, stderr)
	}
	if out, stderr, err := exec.RunSudo(ctx, "systemctl", "daemon-reload"); err != nil {
		return "", fmt.Errorf("reload systemd: %w (%s %s)", err, out, stderr)
	}
	return "[PASS] Removed managed " + kind + " service and directories\n", nil
}
