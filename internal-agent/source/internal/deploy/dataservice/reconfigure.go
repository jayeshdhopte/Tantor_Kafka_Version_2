package dataservice

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/pkg/api"
)

// A port-only retry preserves the installed distribution, service unit, and all
// non-port properties. It must not silently turn into a binary or cluster upgrade.
func portConfiguration(current, desired string) (string, error) {
	parse := func(content string) (map[string]string, error) {
		values := map[string]string{}
		for _, line := range strings.Split(content, "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			key, value, ok := strings.Cut(line, "=")
			key = strings.TrimSpace(key)
			if !ok || strings.Contains(line, "\\") {
				return nil, fmt.Errorf("unsupported existing property syntax")
			}
			if _, duplicate := values[key]; duplicate {
				return nil, fmt.Errorf("duplicate property %s", key)
			}
			values[key] = strings.TrimSpace(value)
		}
		return values, nil
	}
	old, err := parse(current)
	if err != nil {
		return "", err
	}
	want, err := parse(desired)
	if err != nil {
		return "", err
	}
	if len(old) != len(want) {
		return "", fmt.Errorf("existing configuration differs beyond REST ports; a separate configuration migration is required")
	}
	for key, value := range want {
		previous, exists := old[key]
		if !exists {
			return "", fmt.Errorf("existing configuration is missing %s", key)
		}
		if key != "listeners" && key != "rest.advertised.port" && previous != value {
			return "", fmt.Errorf("%s differs; only REST port changes are supported on an existing installation", key)
		}
		if key == "listeners" && !strings.HasPrefix(previous, "http://0.0.0.0:") {
			return "", fmt.Errorf("existing listener must use http://0.0.0.0 for a port-only update")
		}
	}
	lines := strings.Split(current, "\n")
	for i, line := range lines {
		key, _, ok := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if ok && (key == "listeners" || key == "rest.advertised.port") {
			lines[i] = key + "=" + want[key]
		}
	}
	return strings.Join(lines, "\n"), nil
}

func reconfigurePorts(ctx context.Context, exec executor.Executor, s Settings, task *api.Task) (string, error) {
	file := ConfigFiles(s, "/usr")[0]
	current, detail, err := exec.RunSudo(ctx, "cat", "--", file.Path)
	if err != nil {
		return "", fmt.Errorf("existing runtime has no readable configuration at %s: %w: %s", file.Path, err, detail)
	}
	updated, err := portConfiguration(current, file.Content)
	if err != nil {
		return "", err
	}
	unit, _, err := exec.Run(ctx, "systemctl", "show", s.Kind+".service", "--property=ExecStart", "--value")
	launcher := "connect-distributed.sh"
	if s.Kind == Schema {
		launcher = "schema-registry-start"
	}
	if err != nil || !strings.Contains(unit, s.Install+"/bin/"+launcher+" "+file.Path+" ;") {
		return "", fmt.Errorf("existing %s service does not use the requested runtime/configuration paths", s.Kind)
	}
	endpoint := "http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(s.Port))
	if strings.TrimSpace(updated) == strings.TrimSpace(current) {
		return Verify(ctx, s.Kind, endpoint)
	}
	logs, err := Precheck(ctx, exec, s.Kind, task)
	if err != nil {
		return logs, err
	}
	tmp, err := os.CreateTemp("", "tantor-port-config-")
	if err != nil {
		return logs, err
	}
	defer os.Remove(tmp.Name())
	_, writeErr := tmp.WriteString(updated)
	closeErr := tmp.Close()
	if writeErr != nil {
		return logs, writeErr
	}
	if closeErr != nil {
		return logs, closeErr
	}
	backup, detail, err := exec.RunSudo(ctx, "mktemp", file.Path+".port-backup.XXXXXX")
	backup = strings.TrimSpace(backup)
	if err != nil || !strings.HasPrefix(backup, file.Path+".port-backup.") {
		return logs, fmt.Errorf("cannot create configuration backup: %v %s", err, detail)
	}
	if _, _, err = exec.RunSudo(ctx, "cp", "-p", "--", file.Path, backup); err != nil {
		return logs, err
	}
	// Copy through a sibling and rename so ownership/mode survive and readers
	// never observe a partially written configuration.
	_, detail, err = exec.RunSudo(ctx, "bash", "-c", `set -e; dst=$1; src=$2; tmp=$(mktemp "${dst}.port-update.XXXXXX"); trap 'rm -f -- "$tmp"' EXIT; cp -p -- "$dst" "$tmp"; cat -- "$src" > "$tmp"; mv -fT -- "$tmp" "$dst"`, "--", file.Path, tmp.Name())
	if err == nil {
		_, detail, err = exec.RunSudo(ctx, "systemctl", "restart", s.Kind+".service")
	}
	if err == nil {
		var health string
		health, err = Verify(ctx, s.Kind, endpoint)
		logs += "\n" + health
	}
	if err != nil {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _, restoreErr := exec.RunSudo(rollbackCtx, "cp", "-p", "--", backup, file.Path)
		var restartErr error
		if restoreErr == nil {
			_, _, restartErr = exec.RunSudo(rollbackCtx, "systemctl", "restart", s.Kind+".service")
		}
		return logs, fmt.Errorf("port update failed: %w (%s); backup %s; rollback restore=%v restart=%v", err, detail, backup, restoreErr, restartErr)
	}
	return logs + "\n[PASS] REST port updated; runtime and data preserved. Configuration backup: " + backup, nil
}
