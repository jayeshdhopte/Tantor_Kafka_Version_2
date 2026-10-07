package dataservice

import (
	"context"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/pkg/api"
)

var connectDeploymentLock sync.Mutex

func validateSharedRuntime(ctx context.Context, exec executor.Executor, s Settings, task *api.Task) error {
	version := Value(task.Parameters, "kafka_version", Value(task.Parameters, "connect_version", ""))
	if version != "" && !regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`).MatchString(version) {
		return fmt.Errorf("invalid unified Kafka version %q", version)
	}
	_, detail, err := exec.RunSudo(ctx, "bash", "-c", `set -e
root=$1; plugin=$2; version=$3
test -x "$root/bin/connect-distributed.sh"
test -x "$root/bin/kafka-run-class.sh"
test -d "$root/libs"
found=false
for jar in "$root"/libs/connect-runtime-*.jar; do [ ! -f "$jar" ] || found=true; done
"$found"
test -r "$root/config/connect-distributed.properties"
test -d "$plugin" && test -r "$plugin" && test -x "$plugin"
if [ -n "$version" ]; then
 found=false
 for jar in "$root"/libs/kafka_*-"$version".jar; do [ ! -f "$jar" ] || found=true; done
 "$found"
fi`, "--", s.Install, s.Plugin, version)
	if err != nil {
		return fmt.Errorf("existing unified Kafka installation/version or Connect plugins are unavailable at %s (plugins %s): %s", s.Install, s.Plugin, detail)
	}
	return nil
}

var propertyLine = regexp.MustCompile(`^\s*([^\s=:#!]+)\s*(?:[=:]\s*|\s+)(.*)$`)

// Preserve comments and unmanaged properties, replacing all managed key occurrences.
func mergeConnectProperties(current, desired string) (string, error) {
	want := map[string]string{}
	var order []string
	for _, line := range strings.Split(desired, "\n") {
		if m := propertyLine.FindStringSubmatch(line); m != nil {
			want[m[1]] = m[2]
			order = append(order, m[1])
		}
	}
	var result []string
	seen := map[string]bool{}
	continued := false
	for _, line := range strings.Split(strings.TrimSuffix(strings.ReplaceAll(current, "\r\n", "\n"), "\n"), "\n") {
		m := propertyLine.FindStringSubmatch(line)
		if !continued && m != nil {
			if value, managed := want[m[1]]; managed {
				if strings.HasSuffix(strings.TrimRight(line, " \t"), `\`) {
					return "", fmt.Errorf("managed property %s uses unsupported continuation syntax", m[1])
				}
				if !seen[m[1]] {
					result = append(result, m[1]+"="+value)
					seen[m[1]] = true
				}
				continue
			}
		}
		result = append(result, line)
		trimmed := strings.TrimRight(line, " \t")
		slashes := len(trimmed) - len(strings.TrimRight(trimmed, `\`))
		continued = slashes%2 == 1
	}
	if continued {
		return "", fmt.Errorf("existing properties end in an unfinished continuation")
	}
	for _, key := range order {
		if !seen[key] {
			result = append(result, key+"="+want[key])
			seen[key] = true
		}
	}
	return strings.Join(result, "\n") + "\n", nil
}

func deploySharedConnect(ctx context.Context, exec executor.Executor, s Settings, task *api.Task) (logs string, err error) {
	if err = validateSharedRuntime(ctx, exec, s, task); err != nil {
		return "", err
	}
	unitPath := "/etc/systemd/system/kafka-connect.service"
	if _, _, e := exec.RunSudo(ctx, "test", "-e", unitPath); e == nil {
		unit, _, e := exec.RunSudo(ctx, "cat", unitPath)
		if e != nil || !strings.Contains(unit, "Description=Tantor kafka-connect\n") {
			return "", fmt.Errorf("existing Connect unit is not Tantor-managed")
		}
		return reconfigurePorts(ctx, exec, s, task)
	}
	if _, _, e := exec.Run(ctx, "systemctl", "is-active", "--quiet", "kafka-connect.service"); e == nil {
		return "", fmt.Errorf("another Connect service is active")
	}
	if _, _, e := exec.Run(ctx, "pgrep", "-f", "org.apache.kafka.connect.cli.ConnectDistributed"); e == nil {
		return "", fmt.Errorf("another distributed Connect worker is running; stop or migrate it explicitly")
	}
	logs, err = Precheck(ctx, exec, Connect, task)
	if err != nil {
		return logs, err
	}
	out, detail, err := exec.Run(ctx, "bash", "-c", "command -v java | xargs readlink -f")
	javaHome := strings.TrimSuffix(strings.TrimSpace(out), "/bin/java")
	if err != nil || SafePath(javaHome) != nil {
		return logs, fmt.Errorf("cannot locate Java home: %s", detail)
	}
	files := ConfigFiles(s, javaHome)
	if _, _, err = exec.RunSudo(ctx, "bash", "-c", `test -f "$1" && test ! -L "$1"`, "--", files[0].Path); err != nil {
		return logs, fmt.Errorf("Connect properties must be an existing regular file, not a symlink")
	}
	current, detail, err := exec.RunSudo(ctx, "cat", "--", files[0].Path)
	if err != nil {
		return logs, fmt.Errorf("read existing Connect properties: %w: %s", err, detail)
	}
	files[0].Content, err = mergeConnectProperties(current, files[0].Content)
	if err != nil {
		return logs, err
	}
	backup, detail, err := exec.RunSudo(ctx, "mktemp", files[0].Path+".tantor-backup.XXXXXX")
	backup = strings.TrimSpace(backup)
	if err != nil || !strings.HasPrefix(backup, files[0].Path+".tantor-backup.") {
		return logs, fmt.Errorf("cannot create Connect config backup: %v %s", err, detail)
	}
	if _, _, err = exec.RunSudo(ctx, "cp", "-p", "--", files[0].Path, backup); err != nil {
		return logs, err
	}
	logs += "\n[PASS] Connect configuration backup: " + backup
	defer func() {
		if err == nil {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _, stopErr := exec.RunSudo(rollbackCtx, "systemctl", "stop", "kafka-connect.service")
		_, _, disableErr := exec.RunSudo(rollbackCtx, "systemctl", "disable", "kafka-connect.service")
		_, _, restoreErr := exec.RunSudo(rollbackCtx, "cp", "-p", "--", backup, files[0].Path)
		_, _, removeErr := exec.RunSudo(rollbackCtx, "rm", "-f", "--", unitPath)
		_, _, reloadErr := exec.RunSudo(rollbackCtx, "systemctl", "daemon-reload")
		err = fmt.Errorf("%w; rollback stop=%v disable=%v restore=%v remove=%v reload=%v; backup %s", err, stopErr, disableErr, restoreErr, removeErr, reloadErr, backup)
	}()
	dirs := []string{s.Config, s.Log, s.Working}
	if _, _, err = exec.RunSudo(ctx, "mkdir", append([]string{"-p", "--"}, dirs...)...); err != nil {
		return logs, err
	}
	if _, _, err = exec.RunSudo(ctx, "chown", "--", s.User+":"+s.Group, s.Log, s.Working); err != nil {
		return logs, err
	}
	for i, file := range files {
		tmp, e := os.CreateTemp("", "tantor-connect-config-")
		if e != nil {
			return logs, e
		}
		_, e = tmp.WriteString(file.Content)
		closeErr := tmp.Close()
		if e == nil {
			e = closeErr
		}
		if e == nil {
			if i == 0 {
				_, detail, e = exec.RunSudo(ctx, "bash", "-c", `set -e; dst=$1; src=$2; test ! -L "$dst"; tmp=$(mktemp "${dst}.tantor-update.XXXXXX"); trap 'rm -f -- "$tmp"' EXIT; cp -p -- "$dst" "$tmp"; cat -- "$src" > "$tmp"; mv -fT -- "$tmp" "$dst"`, "--", file.Path, tmp.Name())
			} else {
				_, detail, e = exec.RunSudo(ctx, "install", "-m", "0644", "--", tmp.Name(), file.Path)
			}
		}
		os.Remove(tmp.Name())
		if e != nil {
			return logs, fmt.Errorf("write %s: %w: %s", file.Path, e, detail)
		}
	}
	for _, action := range []string{"daemon-reload", "enable", "start"} {
		args := []string{action}
		if action != "daemon-reload" {
			args = append(args, "kafka-connect.service")
		}
		if _, detail, err = exec.RunSudo(ctx, "systemctl", args...); err != nil {
			return logs, fmt.Errorf("%s Connect: %w: %s", action, err, detail)
		}
	}
	health, e := Verify(ctx, Connect, "http://"+net.JoinHostPort("127.0.0.1", strconv.Itoa(s.Port)))
	logs += "\n" + health
	if e != nil {
		return logs, e
	}
	return logs + "\n[PASS] Connect uses existing unified Kafka; runtime, plugins and unrelated properties preserved", nil
}
