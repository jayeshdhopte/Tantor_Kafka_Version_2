// Package configbackup expires only agent-owned Kafka configuration checkpoints.
package configbackup

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"io.translab/tantor-agent/internal/executor"
)

const Registry = "/var/lib/tantor-agent/config-backup-index"
const Retention = 7 * 24 * time.Hour
const Timestamp = "20060102T150405.000000000Z"

var backupName = regexp.MustCompile(`^v[A-Za-z0-9._-]+-([0-9]{8}T[0-9]{6}\.[0-9]{9}Z)\.(bak|dynamic\.json)$`)

// Registration persists across agent restarts. Only root can modify the index.
func Register(ctx context.Context, exec executor.Executor, directory string) error {
	_, detail, err := exec.RunSudo(ctx, "bash", "-c", `set -e
dir=$(readlink -f -- "$1"); test -d "$dir"
name=$(printf '%s' "$dir" | sha256sum); name=${name%% *}.path
test ! -L "$2"
install -d -m 0700 -- "$2"
tmp=$(mktemp "$2/.register.XXXXXX"); trap 'rm -f -- "$tmp"' EXIT
printf '%s\n' "$dir" > "$tmp"; chmod 0600 "$tmp"
mv -fT -- "$tmp" "$2/$name"`, "--", directory, Registry)
	if err != nil {
		return fmt.Errorf("register seven-day configuration backup retention: %w (%s)", err, detail)
	}
	return nil
}

// Sweep uses the creation timestamp in the filename, not the original config's
// preserved mtime or the time a backup was last read. Symlinks are never followed.
func Sweep(registry string, now time.Time) error {
	entries, err := os.ReadDir(registry)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var failures []string
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".path") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(registry, entry.Name()))
		if err != nil {
			failures = append(failures, err.Error())
			continue
		}
		dir := strings.TrimSpace(string(data))
		if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir || filepath.Base(filepath.Dir(dir)) != ".tantor-backups" {
			failures = append(failures, "invalid backup index path")
			continue
		}
		real, err := filepath.EvalSymlinks(dir)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil || real != dir {
			failures = append(failures, "redirected backup path: "+dir)
			continue
		}
		files, err := os.ReadDir(dir)
		if err != nil {
			failures = append(failures, err.Error())
			continue
		}
		for _, file := range files {
			if !file.Type().IsRegular() {
				continue
			}
			match := backupName.FindStringSubmatch(file.Name())
			if match == nil {
				continue
			}
			created, err := time.Parse(Timestamp, match[1])
			if err != nil || !created.Before(now.Add(-Retention)) {
				continue
			}
			if err := os.Remove(filepath.Join(dir, file.Name())); err != nil {
				failures = append(failures, err.Error())
			}
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("config backup retention: %s", strings.Join(failures, "; "))
	}
	return nil
}

func Start(ctx context.Context) {
	if os.Geteuid() != 0 {
		return
	}
	go func() {
		clean := func() {
			if err := Sweep(Registry, time.Now().UTC()); err != nil {
				slog.Warn("Configuration backup cleanup failed", "err", err)
			}
		}
		clean()
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				clean()
			}
		}
	}()
}
