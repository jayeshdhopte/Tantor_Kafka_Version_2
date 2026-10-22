package logrotation

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
)

// PruneCommand intentionally needs no network connection or backend credential.
func PruneCommand(args []string, defaultDir, name string) (bool, error) {
	requested := false
	dir := defaultDir
	for i, arg := range args {
		if arg == "--prune-logs" {
			requested = true
		}
		if strings.HasPrefix(arg, "--log-dir=") {
			dir = strings.TrimPrefix(arg, "--log-dir=")
		}
		if arg == "--log-dir" && i+1 < len(args) {
			dir = args[i+1]
		}
		if strings.HasPrefix(arg, "--log-name=") {
			name = strings.TrimPrefix(arg, "--log-name=")
		}
	}
	if !requested {
		return false, nil
	}
	return true, Prune(dir, name, time.Now())
}

func RetentionUnits(binary, dir, name, prefix string) (string, string, error) {
	if err := validDirectory(dir); err != nil {
		return "", "", err
	}
	if !filepath.IsAbs(binary) || strings.ContainsAny(binary+dir, "\r\n\x00%\"\\") {
		return "", "", fmt.Errorf("unsafe retention service path")
	}
	if !regexp.MustCompile(`^tantor-[a-zA-Z0-9_-]{1,100}$`).MatchString(prefix) || filepath.Base(name) != name || !regexp.MustCompile(`^[a-zA-Z0-9._-]+\.log$`).MatchString(name) {
		return "", "", fmt.Errorf("invalid managed log retention identity")
	}
	unit := fmt.Sprintf("[Unit]\nDescription=Tantor application log archive retention\n[Service]\nType=oneshot\nExecStart=\"%s\" --prune-logs --log-dir=\"%s\" --log-name=%s\n", binary, dir, name)
	timer := "[Unit]\nDescription=Prune Tantor agent archives older than seven days\n[Timer]\nOnCalendar=hourly\nPersistent=true\n[Install]\nWantedBy=timers.target\n"
	return unit, timer, nil
}

// InstallRetention is idempotent and does not restart the running agent.
func InstallRetention(ctx context.Context, dir, name, prefix string) error {
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		return nil
	}
	binary, err := os.Executable()
	if err != nil {
		return err
	}
	unit, timer, err := RetentionUnits(binary, dir, name, prefix)
	if err != nil {
		return err
	}
	changed := false
	for suffix, content := range map[string]string{".service": unit, ".timer": timer} {
		path := filepath.Join("/etc/systemd/system", prefix+"-log-retention"+suffix)
		if old, err := os.ReadFile(path); err == nil && string(old) == content {
			continue
		}
		if info, err := os.Lstat(path); err == nil && !info.Mode().IsRegular() {
			return fmt.Errorf("unsafe retention unit")
		}
		if err = os.WriteFile(path, []byte(content), 0644); err != nil {
			return err
		}
		changed = true
	}
	if changed {
		if out, err := exec.CommandContext(ctx, "systemctl", "daemon-reload").CombinedOutput(); err != nil {
			return fmt.Errorf("reload retention units: %w: %s", err, out)
		}
	}
	if out, err := exec.CommandContext(ctx, "systemctl", "enable", "--now", prefix+"-log-retention.timer").CombinedOutput(); err != nil {
		return fmt.Errorf("start retention timer: %w: %s", err, out)
	}
	return nil
}
