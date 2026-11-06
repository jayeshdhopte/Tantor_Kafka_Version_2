//go:build linux

package expansion

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"io.translab/tantor-agent/internal/executor"
)

// RunParticipant only acts on exact units/configurations frozen in a static
// parent operation. It has no cleanup, formatting or arbitrary command action.
func RunParticipant(ctx context.Context, hostID, base string, params map[string]string, e executor.Executor) (string, error) {
	if os.Geteuid() != 0 {
		return "", fmt.Errorf("static participant adapter requires a root Linux internal agent")
	}
	p, err := Decode(params["expansion_plan"], params["plan_digest"], hostID)
	if err != nil {
		return "", err
	}
	if p.QuorumMode != "STATIC" {
		return "", fmt.Errorf("dynamic expansion cannot mutate existing participant units")
	}
	if !safePath(base) {
		return "", fmt.Errorf("approved native data directory required")
	}
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	root := filepath.Join(base, "quorum-expansion")
	if err = privateDirectory(root); err != nil {
		return "", err
	}
	lock, err := os.OpenFile(filepath.Join(root, "host.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return "", err
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return "", fmt.Errorf("another expansion task owns this host")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	manifest, err := loadManifest(ctx, p)
	if err != nil {
		return "", err
	}
	dir := filepath.Join(root, p.OperationID)
	if err = privateDirectory(dir); err != nil {
		return "", err
	}
	env := &nativeParticipantEnvironment{dir: dir, exec: e, manifest: manifest}
	report, err := executeParticipant(ctx, p, params["plan_digest"], hostID, params["participant_unit"], params["action"], env)
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(report)
	return string(raw), err
}

type nativeParticipantEnvironment struct {
	dir      string
	exec     executor.Executor
	manifest ArtifactManifest
}

func (e *nativeParticipantEnvironment) file(m Participant, name string) string {
	return filepath.Join(e.dir, Digest(m.Unit)+"-"+name)
}
func (e *nativeParticipantEnvironment) LoadParticipantReceipt(m Participant) (*ParticipantReceipt, error) {
	raw, err := readBounded(context.Background(), e.file(m, "receipt.json"), 32768)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var r ParticipantReceipt
	if json.Unmarshal([]byte(raw), &r) != nil {
		return nil, fmt.Errorf("invalid native participant receipt")
	}
	return &r, nil
}
func (e *nativeParticipantEnvironment) SaveParticipantReceipt(r ParticipantReceipt) error {
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return durableReplace(e.file(Participant{Unit: r.Unit}, "receipt.json"), string(raw), 0600, nil)
}
func (e *nativeParticipantEnvironment) ReadOriginal(ctx context.Context, m Participant) (string, error) {
	raw, err := readBounded(ctx, e.file(m, "original.properties"), 262144)
	if os.IsNotExist(err) {
		return readBounded(ctx, m.ConfigPath, 262144)
	}
	if err != nil {
		return "", err
	}
	if Digest(raw) != m.ConfigSHA256 {
		return "", fmt.Errorf("participant backup differs from frozen original")
	}
	return raw, nil
}
func (e *nativeParticipantEnvironment) Stage(ctx context.Context, m Participant, original, expanded string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := writeExclusive(e.file(m, "original.properties"), original, 0600); err != nil {
		return err
	}
	return writeExclusive(e.file(m, "expanded.properties"), expanded, 0600)
}
func (e *nativeParticipantEnvironment) StopParticipant(ctx context.Context, m Participant) error {
	_, _, err := e.exec.Run(ctx, "systemctl", "stop", m.Unit)
	return err
}
func (e *nativeParticipantEnvironment) StartParticipant(ctx context.Context, m Participant) error {
	_, _, err := e.exec.Run(ctx, "systemctl", "start", m.Unit)
	return err
}
func (e *nativeParticipantEnvironment) ApplyParticipant(ctx context.Context, m Participant, original, expanded string) error {
	// Recheck the exact stopped unit and config immediately before replacement.
	state, err := e.show(ctx, m.Unit)
	if err != nil || !stoppedUnit(state) {
		return fmt.Errorf("participant is not stopped at configuration apply")
	}
	f, info, err := openRegular(m.ConfigPath, 262144)
	if err != nil {
		return err
	}
	defer f.Close()
	raw, err := readBounded(ctx, m.ConfigPath, 262144)
	if err != nil || raw != original {
		return fmt.Errorf("participant original changed before replacement")
	}
	if err = unchanged(m.ConfigPath, f, info); err != nil {
		return err
	}
	return durableReplace(m.ConfigPath, expanded, info.Mode().Perm(), info)
}
func (e *nativeParticipantEnvironment) show(ctx context.Context, unitName string) (map[string]string, error) {
	out, _, err := e.exec.Run(ctx, "systemctl", "show", "--no-pager", "--property=Id,LoadState,ActiveState,SubState,MainPID,FragmentPath,ExecMainStartTimestampMonotonic,DropInPaths", unitName)
	if err != nil || len(out) > 32768 {
		return nil, fmt.Errorf("participant unit observation unavailable")
	}
	values := map[string]string{}
	for _, line := range strings.Split(out, "\n") {
		kv := strings.SplitN(line, "=", 2)
		if len(kv) == 2 {
			values[kv[0]] = kv[1]
		}
	}
	if values["Id"] != unitName || values["LoadState"] != "loaded" || values["DropInPaths"] != "" {
		return nil, fmt.Errorf("participant unit or drop-ins differ from supported inspection")
	}
	return values, nil
}
func stoppedUnit(values map[string]string) bool {
	return values["MainPID"] == "0" && (values["ActiveState"] == "inactive" || values["ActiveState"] == "failed")
}
func (e *nativeParticipantEnvironment) ObserveParticipant(ctx context.Context, p Plan, m Participant) (ParticipantObservation, error) {
	var o ParticipantObservation
	first, err := e.show(ctx, m.Unit)
	if err != nil {
		return o, err
	}
	fragment := first["FragmentPath"]
	if filepath.Base(fragment) != m.Unit || !(strings.HasPrefix(fragment, "/etc/systemd/system/") || strings.HasPrefix(fragment, "/usr/lib/systemd/system/") || strings.HasPrefix(fragment, "/lib/systemd/system/")) {
		return o, fmt.Errorf("unreviewed unit fragment")
	}
	o.UnitSHA256, err = fileSHA256(ctx, fragment, 262144)
	if err != nil {
		return o, err
	}
	o.ConfigSHA256, err = fileSHA256(ctx, m.ConfigPath, 262144)
	if err != nil {
		return o, err
	}
	o.Stopped = stoppedUnit(first)
	o.Active = first["ActiveState"] == "active" && first["SubState"] == "running"
	o.PID, _ = strconv.Atoi(first["MainPID"])
	if o.Active && o.PID < 1 {
		return o, fmt.Errorf("active participant PID unavailable")
	}
	identities := map[string]bool{}
	o.StorageVerified = true
	for _, s := range m.Storage {
		raw, err := readBounded(ctx, filepath.Join(s.Path, "meta.properties"), 16384)
		if err != nil {
			return o, err
		}
		props, err := simpleProperties(raw)
		if err != nil || props["cluster.id"] != p.KafkaClusterID || props["node.id"] != strconv.Itoa(m.NodeID) || props["directory.id"] != s.DirectoryID || identities[s.DirectoryID] {
			o.StorageVerified = false
		}
		identities[s.DirectoryID] = true
	}
	if o.Active {
		proc := filepath.Join("/proc", strconv.Itoa(o.PID))
		cmd, err := readBounded(ctx, filepath.Join(proc, "cmdline"), 65536)
		if err != nil {
			return o, err
		}
		stat, err := readBounded(ctx, filepath.Join(proc, "stat"), 16384)
		if err != nil {
			return o, err
		}
		o.StartTicks = processStartTicks(stat)
		if o.StartTicks == "" {
			return o, fmt.Errorf("participant process start identity unavailable")
		}
		args := strings.Split(strings.TrimSuffix(cmd, "\x00"), "\x00")
		proof, err := VerifyRuntime(ctx, e.manifest, m.RuntimeHome, m.ConfigPath, args, m.JavaAgents, diskRuntimeFiles{})
		if err != nil {
			return o, err
		}
		o.RuntimeVerified = proof.ProofSHA256 == m.ArtifactProofSHA256
		again, err := readBounded(ctx, filepath.Join(proc, "cmdline"), 65536)
		if err != nil || again != cmd {
			return o, fmt.Errorf("participant command changed while observing")
		}
		againStat, err := readBounded(ctx, filepath.Join(proc, "stat"), 16384)
		if err != nil || processStartTicks(againStat) != o.StartTicks {
			return o, fmt.Errorf("participant PID was reused while observing")
		}
	}
	if raw, err := readBounded(ctx, e.file(m, "original.properties"), 262144); err == nil {
		o.BackupVerified = Digest(raw) == m.ConfigSHA256
		if expanded, err := ExpandedConfig(p, m, raw); err == nil {
			staged, err := readBounded(ctx, e.file(m, "expanded.properties"), 262144)
			o.StageVerified = err == nil && staged == expanded
		}
	}
	second, err := e.show(ctx, m.Unit)
	if err != nil || first["MainPID"] != second["MainPID"] || first["ExecMainStartTimestampMonotonic"] != second["ExecMainStartTimestampMonotonic"] || first["ActiveState"] != second["ActiveState"] || first["SubState"] != second["SubState"] || first["FragmentPath"] != second["FragmentPath"] {
		return o, fmt.Errorf("participant service changed during observation")
	}
	configHash, err := fileSHA256(ctx, m.ConfigPath, 262144)
	if err != nil || configHash != o.ConfigSHA256 {
		return o, fmt.Errorf("participant configuration changed during observation")
	}
	unitHash, err := fileSHA256(ctx, fragment, 262144)
	if err != nil || unitHash != o.UnitSHA256 {
		return o, fmt.Errorf("participant unit changed during observation")
	}
	return o, nil
}
func processStartTicks(stat string) string {
	split := strings.LastIndex(stat, ") ")
	if split < 0 {
		return ""
	}
	fields := strings.Fields(stat[split+2:])
	if len(fields) < 20 {
		return ""
	}
	if _, err := strconv.ParseUint(fields[19], 10, 64); err != nil || fields[19] == "0" {
		return ""
	}
	return fields[19]
}
func simpleProperties(raw string) (map[string]string, error) {
	out := map[string]string{}
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "!") {
			continue
		}
		if strings.ContainsAny(line, "\\\x00") {
			return nil, fmt.Errorf("unsupported escaped properties")
		}
		kv := strings.SplitN(line, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("ambiguous property")
		}
		key, value := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
		if !propertyKey.MatchString(key) {
			return nil, fmt.Errorf("invalid property key")
		}
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("duplicate property key")
		}
		out[key] = value
	}
	return out, nil
}
func privateDirectory(dir string) error {
	if err := noLinks(dir); err == nil {
		info, err := os.Stat(dir)
		if err != nil || !info.IsDir() {
			return fmt.Errorf("private directory required")
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	parent := filepath.Dir(dir)
	if parent == dir {
		return fmt.Errorf("missing directory root")
	}
	if err := privateDirectory(parent); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0700); err != nil && !os.IsExist(err) {
		return err
	}
	return noLinks(dir)
}
func writeExclusive(name, content string, mode os.FileMode) error {
	if err := noLinks(filepath.Dir(name)); err != nil {
		return err
	}
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, mode)
	if os.IsExist(err) {
		raw, err := readBounded(context.Background(), name, 262144)
		if err != nil || raw != content {
			return fmt.Errorf("owned staged file differs")
		}
		return nil
	}
	if err != nil {
		return err
	}
	_, err = f.WriteString(content)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return syncDirectory(filepath.Dir(name))
}
func durableReplace(name, content string, mode os.FileMode, expected os.FileInfo) error {
	if err := noLinks(filepath.Dir(name)); err != nil {
		return err
	}
	if info, err := os.Lstat(name); err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("regular replacement target required")
		}
		if expected != nil && !os.SameFile(expected, info) {
			return fmt.Errorf("replacement target inode changed")
		}
	} else if !os.IsNotExist(err) {
		return err
	} else if expected != nil {
		return fmt.Errorf("replacement target disappeared")
	}
	f, err := os.CreateTemp(filepath.Dir(name), ".tantor-expansion-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(mode); err != nil {
		f.Close()
		return err
	}
	if expected != nil {
		stat, ok := expected.Sys().(*syscall.Stat_t)
		if !ok || stat.Nlink != 1 {
			f.Close()
			return fmt.Errorf("single-link Linux configuration required")
		}
		if err = f.Chown(int(stat.Uid), int(stat.Gid)); err != nil {
			f.Close()
			return err
		}
	}
	_, err = f.WriteString(content)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if expected != nil {
		current, err := os.Lstat(name)
		if err != nil || !os.SameFile(expected, current) || current.Size() != expected.Size() || current.ModTime() != expected.ModTime() {
			return fmt.Errorf("replacement target changed before commit")
		}
	}
	if err = os.Rename(tmp, name); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(name))
}
func syncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
