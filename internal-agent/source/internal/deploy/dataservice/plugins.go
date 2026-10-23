package dataservice

import (
	"context"
	"fmt"
	"io.translab/tantor-agent/internal/executor"
)

// Both agents use this installer. FileStream is packaged separately from the
// Connect runtime in unified parcels and must be on the worker's plugin path.
// Use the selected distribution only; never download a different Kafka version.
func installBundledFileStream(ctx context.Context, run executor.Executor, source string, s Settings) error {
	if s.Kind != Connect {
		return nil
	}
	_, detail, err := run.RunSudo(ctx, "bash", "-c", bundledFileStreamShell, "--", source, s.Plugin)
	if err != nil {
		return fmt.Errorf("install bundled FileStream Source/Sink plugins: %w: %s", err, detail)
	}
	return nil
}

const bundledFileStreamShell = `set -eu
root=$(readlink -f -- "$1"); destination=$2/tantor-filestream
test -d "$root"
test ! -L "$2"; test ! -L "$destination"
found=''
for tree in libs share connect-plugins plugins; do
 test ! -d "$root/$tree" || {
  candidates=$(find -L "$root/$tree" -type f -name 'connect-file-*.jar' -print)
  if [ -n "$candidates" ]; then found="$candidates"; break; fi
 }
done
if [ -z "$found" ]; then
 echo 'Selected Kafka distribution does not include connect-file-*.jar; use the same unified parcel as the internal cluster.' >&2
 exit 1
fi
# Multiple versions in a single distribution are ambiguous and unsafe to load.
count=$(printf '%s\n' "$found" | wc -l)
test "$count" -eq 1 || { echo 'Multiple bundled FileStream JARs found' >&2; exit 1; }
jar=$(readlink -f -- "$found")
case "$jar" in "$root"/*) ;; *) echo 'FileStream JAR escapes selected distribution' >&2; exit 1;; esac
test -r "$jar"
# Do not duplicate an already installed copy or overwrite a custom version.
if [ -d "$2" ]; then
 existing=$(find -L "$2" -type f -name 'connect-file-*.jar' -print)
 if [ -n "$existing" ]; then
  while IFS= read -r installed; do
   cmp -s -- "$jar" "$installed" || { echo 'Configured plugin path already has a different FileStream version; explicit upgrade required' >&2; exit 1; }
  done <<EOF
$existing
EOF
  exit 0
 fi
fi
install -d -m 0755 -- "$destination"
target=$destination/$(basename -- "$jar")
test ! -L "$target"
if [ -e "$target" ]; then
 cmp -s -- "$jar" "$target" || { echo 'Existing FileStream plugin differs; explicit upgrade required' >&2; exit 1; }
else
 install -m 0644 -- "$jar" "$target"
fi
`
