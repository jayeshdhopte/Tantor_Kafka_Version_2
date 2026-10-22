package logrotation

import (
	"context"
	"fmt"
	"io.translab/tantor-agent/internal/executor"
	"os"
	"path/filepath"
)

// RegisterApplicationRetention is shared by internal and external deployments.
// Its independent timer still prunes archives when the application is stopped.
func RegisterApplicationRetention(ctx context.Context, run executor.Executor, dir, name, prefix string) error {
	binary, err := os.Executable()
	if err != nil {
		return err
	}
	service, timer, err := RetentionUnits(binary, dir, name, prefix)
	if err != nil {
		return err
	}
	for suffix, body := range map[string]string{".service": service, ".timer": timer} {
		temp, err := os.CreateTemp("", "tantor-log-retention-")
		if err != nil {
			return err
		}
		_, err = temp.WriteString(body)
		closeErr := temp.Close()
		if err == nil {
			err = closeErr
		}
		if err == nil {
			dst := filepath.Join("/etc/systemd/system", prefix+"-log-retention"+suffix)
			_, detail, e := run.RunSudo(ctx, "bash", "-c", `set -e; test ! -L "$2"; install -m 0644 -- "$1" "$2"`, "--", temp.Name(), dst)
			if e != nil {
				err = fmt.Errorf("install application log retention: %w: %s", e, detail)
			}
		}
		os.Remove(temp.Name())
		if err != nil {
			return err
		}
	}
	if _, detail, err := run.RunSudo(ctx, "systemctl", "daemon-reload"); err != nil {
		return fmt.Errorf("reload log retention: %w: %s", err, detail)
	}
	if _, detail, err := run.RunSudo(ctx, "systemctl", "enable", "--now", prefix+"-log-retention.timer"); err != nil {
		return fmt.Errorf("enable log retention: %w: %s", err, detail)
	}
	return nil
}
