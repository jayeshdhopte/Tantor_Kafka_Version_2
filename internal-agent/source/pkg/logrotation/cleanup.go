package logrotation

import (
	"context"
	"fmt"
	"io.translab/tantor-agent/internal/executor"
)

// RemoveApplicationRetention removes the exact application's pair, including
// partially installed pairs. Agent-wide retention is deliberately excluded.
func RemoveApplicationRetention(ctx context.Context, run executor.Executor, dir, name, prefix string, otherDirs ...string) error {
	if prefix == "tantor-agent" || prefix == "tantor-discovery-agent" {
		return fmt.Errorf("cluster cleanup cannot remove agent retention")
	}
	dirs := append([]string{dir}, otherDirs...)
	for _, candidate := range dirs {
		if _, _, err := RetentionUnits("/usr/bin/tantor", candidate, name, prefix); err != nil {
			return err
		}
	}
	args := append([]string{"-c", removeApplicationRetentionShell, "--", "/etc/systemd/system", prefix, name}, dirs...)
	_, detail, err := run.RunSudo(ctx, "bash", args...)
	if err != nil {
		return fmt.Errorf("remove application log retention %s: %w: %s", prefix, err, detail)
	}
	return nil
}

const removeApplicationRetentionShell = `set -eu
base=$1; prefix=$2; name=$3; shift 3
service=$base/$prefix-log-retention.service
timer=$base/$prefix-log-retention.timer
# Check ownership before stopping anything. A missing half is a valid retry.
for file in "$service" "$timer"; do
 test ! -L "$file" || { echo 'Refusing redirected retention unit' >&2; exit 1; }
 if [ -e "$file" ]; then test -f "$file" || exit 1; fi
done
if [ -f "$service" ]; then
 grep -qxF 'Description=Tantor application log archive retention' "$service"
 matched=false
 for dir in "$@"; do
  if grep -qF -- " --prune-logs --log-dir=\"$dir\" --log-name=$name" "$service"; then matched=true; fi
 done
 "$matched" || { echo 'Retention log directory differs from deployment' >&2; exit 1; }
fi
if [ -f "$timer" ]; then
 grep -qxF 'Description=Prune Tantor agent archives older than seven days' "$timer"
 grep -qxF 'OnCalendar=hourly' "$timer"
fi
if [ ! -e "$service" ] && [ ! -e "$timer" ]; then exit 0; fi
# Stop the timer first, then wait for a running prune process to finish/stop.
for suffix in timer service; do
 unit=$prefix-log-retention.$suffix
 state=$(systemctl show "$unit" --property=LoadState --value)
 if [ "$state" != not-found ]; then
  systemctl stop "$unit"
  systemctl disable "$unit"
 fi
done
rm -rf -- "$timer.d" "$service.d"
rm -f -- "$timer" "$service"
systemctl daemon-reload
`
