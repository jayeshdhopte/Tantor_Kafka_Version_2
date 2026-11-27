package javaupgrade

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"io.translab/tantor-agent/internal/taskvalidate"
	"io.translab/tantor-agent/pkg/api"
)

// Executor is supplied by the enrolled internal or external agent.
type Executor interface {
	Run(context.Context, string, ...string) (string, string, error)
	RunSudo(context.Context, string, ...string) (string, string, error)
}

var javaUpgradeLock sync.Mutex
var javaVersionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
var javaArtifactPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$`)
var javaChecksumPattern = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)

func Run(ctx context.Context, executor Executor, task *api.Task, log *strings.Builder) error {
	return run(ctx, executor, task, log, "")
}

// RunDiscoveredService is for a locally verified and allowlisted external systemd unit.
// The external adapter must verify its ExecStart/config binding before calling this.
func RunDiscoveredService(ctx context.Context, executor Executor, task *api.Task, log *strings.Builder, unit string) error {
	if !regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.@:-]{0,199}\.service$`).MatchString(unit) || strings.TrimSuffix(unit, ".service") != task.Parameters["service_name"] {
		return fmt.Errorf("invalid discovered Kafka service identity")
	}
	return run(ctx, executor, task, log, unit)
}

func run(ctx context.Context, executor Executor, task *api.Task, log *strings.Builder, discoveredUnit string) (err error) {
	if runtime.GOOS != "linux" {
		return fmt.Errorf("Java upgrade requires Linux")
	}
	if !javaUpgradeLock.TryLock() {
		return fmt.Errorf("another Java upgrade is running on this host")
	}
	defer javaUpgradeLock.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	p := task.Parameters
	version, id := p["target_version"], p["artifact_id"]
	digest := task.Checksum
	if !javaVersionPattern.MatchString(version) || !javaArtifactPattern.MatchString(id) || !javaChecksumPattern.MatchString(digest) {
		return fmt.Errorf("exact Java version, artifact UUID and catalog SHA-256 are required")
	}
	unit := discoveredUnit
	if unit == "" {
		unit, err = taskvalidate.KafkaServiceName(p["service_name"], "service_name")
		if err != nil || unit == "zookeeper" {
			return fmt.Errorf("a Kafka systemd service is required")
		}
		unit += ".service"
	}
	base, err := taskvalidate.ApprovedPath(p["parcel_dir"])
	if err != nil {
		return err
	}
	run := func(c context.Context, cmd string, args ...string) (string, error) {
		out, stderr, runErr := executor.RunSudo(c, cmd, args...)
		if runErr != nil {
			return "", fmt.Errorf("%s: %w: %s", cmd, runErr, stderr)
		}
		return strings.TrimSpace(out), nil
	}
	services := []ServiceTarget{{Name: unit, Role: p["service_role"], KafkaHome: p["kafka_home"], Endpoint: p["health_endpoint"]}}
	if raw := p["separate_services"]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &services); err != nil {
			return fmt.Errorf("invalid separate Kafka services: %w", err)
		}
		if err := validateSeparateServices(services, unit); err != nil {
			return err
		}
	}
	activeUnits := []ServiceTarget{}
	allowedPIDs := map[string]bool{}
	for _, service := range services {
		if err := validateServiceTarget(service); err != nil {
			return err
		}
		loaded, e := run(ctx, "systemctl", "show", "--property=LoadState", "--value", "--", service.Name)
		if e != nil || loaded != "loaded" {
			return fmt.Errorf("Kafka service %s is not loaded", service.Name)
		}
		state, e := run(ctx, "systemctl", "show", "--property=ActiveState", "--value", "--", service.Name)
		if e != nil || (state != "active" && state != "inactive") {
			return fmt.Errorf("cannot establish stable Kafka service state for %s", service.Name)
		}
		if state == "active" {
			pid, e := run(ctx, "systemctl", "show", "--property=MainPID", "--value", "--", service.Name)
			if e != nil || !regexp.MustCompile(`^[1-9][0-9]*$`).MatchString(pid) || allowedPIDs[pid] {
				return fmt.Errorf("Kafka MainPID is unavailable or duplicated for %s", service.Name)
			}
			allowedPIDs[pid] = true
			activeUnits = append(activeUnits, service)
		}
	}
	processes, err := run(ctx, "ps", "-eo", "pid=,comm=")
	if err != nil {
		return err
	}
	if err := rejectUnrelatedJava(processes, allowedPIDs); err != nil {
		return err
	}
	active := len(activeUnits) > 0
	probeService := func(c context.Context, service ServiceTarget) error { return probeKafkaService(c, executor, service) }
	for _, service := range activeUnits {
		if err := probeService(ctx, service); err != nil {
			return fmt.Errorf("pre-upgrade health for %s: %w", service.Name, err)
		}
	}
	waitAll := func(c context.Context, binary string) error {
		for _, service := range activeUnits {
			if err := waitForJavaService(c, run, service.Name, binary, func(pc context.Context) error { return probeService(pc, service) }); err != nil {
				return fmt.Errorf("%s: %w", service.Name, err)
			}
		}
		return nil
	}
	const javaLink = "/usr/bin/java"
	previous, err := run(ctx, "readlink", "--", javaLink)
	if err != nil || previous == "" {
		return fmt.Errorf("cannot preserve current Java default: expected %s to be a symbolic link", javaLink)
	}
	previousReal, err := run(ctx, "readlink", "-f", "--", javaLink)
	if err != nil {
		return err
	}
	oldReport, _, reportErr := executor.Run(ctx, javaLink, "--version")
	oldMatch := regexp.MustCompile(`^(?:openjdk|java) ([0-9]+\.[0-9]+\.[0-9]+)(?:\s|$)`).FindStringSubmatch(oldReport)
	if reportErr != nil || len(oldMatch) != 2 {
		return fmt.Errorf("current VM Java version could not be verified")
	}
	oldVersion := oldMatch[1]
	if oldVersion == version {
		if active {
			if err := waitAll(ctx, previousReal); err != nil {
				return err
			}
		}
		fmt.Fprintf(log, "VM default Java is already %s. Kafka service state preserved.\n", version)
		return nil
	}
	if !Allowed(oldVersion, version) {
		return fmt.Errorf("unsupported Java upgrade from %s to %s", oldVersion, version)
	}
	work, err := run(ctx, "mktemp", "-d", "/usr/lib/jvm/.tantor-java-XXXXXXXX")
	if err != nil {
		return err
	}
	if !regexp.MustCompile(`^/usr/lib/jvm/\.tantor-java-[A-Za-z0-9]+$`).MatchString(work) {
		return fmt.Errorf("unexpected JDK work directory")
	}
	keepWork := false
	defer func() {
		if !keepWork {
			cleanCtx, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			if _, cleanErr := run(cleanCtx, "rm", "-rf", "--", work); cleanErr != nil {
				fmt.Fprintf(log, "JDK work directory cleanup failed: %v\n", cleanErr)
			}
		}
	}()
	archive := filepath.Join(work, "jdk.tar.gz")
	staged := filepath.Join(base, ".downloads", "java-"+id+".tar.gz")
	if _, err = run(ctx, "install", "-m", "0600", "--", staged, archive); err != nil {
		return err
	}
	actual, err := run(ctx, "sha256sum", "--", archive)
	if err != nil {
		return err
	}
	fields := strings.Fields(actual)
	if len(fields) == 0 || !strings.EqualFold(fields[0], digest) {
		return fmt.Errorf("staged JDK checksum mismatch")
	}
	// Validate the archive before using privileged extraction. Permit the supplied JDK license links.
	if _, err = run(ctx, "chmod", "0644", "--", archive); err != nil {
		return err
	}
	if _, err = run(ctx, "chmod", "0755", "--", work); err != nil {
		return err
	}
	if err = InspectArchive(archive, version); err != nil {
		return err
	}
	jdk := filepath.Join(work, "jdk")
	if _, err = run(ctx, "mkdir", "--", jdk); err != nil {
		return err
	}
	if _, err = run(ctx, "tar", "--extract", "--gzip", "--file", archive, "--directory", jdk, "--strip-components=1", "--no-same-owner", "--no-same-permissions"); err != nil {
		return err
	}
	if _, err = run(ctx, "rm", "-f", "--", archive); err != nil {
		return err
	}
	binary := filepath.Join(jdk, "bin", "java")
	reported, _, versionErr := executor.Run(ctx, binary, "--version")
	if versionErr != nil || !regexp.MustCompile(`^(openjdk|java) `+regexp.QuoteMeta(version)+`(\s|$)`).MatchString(reported) {
		return fmt.Errorf("staged Java binary did not report target version %s: %v", version, versionErr)
	}
	fmt.Fprintf(log, "Verified and prepared Java %s from the staged package\n", version)
	slog.Info("Java upgrade package verified", "taskId", task.TaskID, "version", version)

	switched, stopped := false, false
	defer func() {
		if err == nil || (!switched && !stopped) {
			return
		}
		recoverCtx, done := context.WithTimeout(context.Background(), 2*time.Minute)
		defer done()
		var recovery []string
		if switched {
			if active {
				if x := transitionServices(recoverCtx, run, activeUnits, "stop"); x != nil {
					recovery = append(recovery, x.Error())
				}
			}
			if x := switchJavaLink(recoverCtx, run, previous); x != nil {
				recovery = append(recovery, x.Error())
				keepWork = true
			}
		}
		if stopped && len(recovery) == 0 {
			if x := transitionServices(recoverCtx, run, activeUnits, "start"); x != nil {
				recovery = append(recovery, x.Error())
			}
			if len(recovery) == 0 {
				verifyCtx, verifyDone := context.WithTimeout(recoverCtx, 90*time.Second)
				if x := waitAll(verifyCtx, previousReal); x != nil {
					recovery = append(recovery, x.Error())
				}
				verifyDone()
			}
		}
		if len(recovery) > 0 {
			err = fmt.Errorf("%w; recovery requires review: %s", err, strings.Join(recovery, "; "))
		} else {
			fmt.Fprintln(log, "Restored previous Java default and Kafka service state")
		}
	}()
	if active {
		stopped = true
		if err = transitionServices(ctx, run, activeUnits, "stop"); err != nil {
			return err
		}
	}
	for _, service := range services {
		if err = verifyStoppedService(ctx, run, service.Name); err != nil {
			return err
		}
	}
	processes, err = run(ctx, "ps", "-eo", "pid=,comm=")
	if err != nil {
		return err
	}
	if err = rejectUnrelatedJava(processes, map[string]bool{}); err != nil {
		return err
	}
	current, err := run(ctx, "readlink", "--", javaLink)
	if err != nil || current != previous {
		return fmt.Errorf("Java default changed during the upgrade")
	}
	switched = true
	if err = switchJavaLink(ctx, run, binary); err != nil {
		return err
	}
	resolved, err := run(ctx, "readlink", "-f", "--", javaLink)
	if err != nil || resolved != binary {
		return fmt.Errorf("Java default does not point to the target JDK")
	}
	if active {
		if err = transitionServices(ctx, run, activeUnits, "start"); err != nil {
			return err
		}
		verifyCtx, done := context.WithTimeout(ctx, 90*time.Second)
		defer done()
		if err = waitAll(verifyCtx, binary); err != nil {
			return fmt.Errorf("Kafka did not verify on target Java %s: %w", version, err)
		}
		fmt.Fprintf(log, "Kafka is running on Java %s and health checks passed\n", version)
	} else {
		fmt.Fprintln(log, "Kafka remains stopped. Verify the running JVM after the next start.")
	}
	keepWork = true
	fmt.Fprintf(log, "VM default Java switched from %s to %s\n", previousReal, binary)
	return nil
}

// systemctl start returns before Kafka finishes starting. Require both the
// expected running JVM and Kafka health, including after restoring the old JDK.
func waitForJavaService(ctx context.Context, run func(context.Context, string, ...string) (string, error), unit, binary string, probe func(context.Context) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("Kafka JVM and health verification: %w", err)
		}
		pid, pidErr := run(ctx, "systemctl", "show", "--property=MainPID", "--value", "--", unit)
		if pidErr == nil && regexp.MustCompile(`^[1-9][0-9]*$`).MatchString(pid) {
			exe, exeErr := run(ctx, "readlink", "-f", "--", "/proc/"+pid+"/exe")
			if exeErr == nil && exe == binary && probe(ctx) == nil {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("Kafka JVM and health verification: %w", ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

func switchJavaLink(ctx context.Context, run func(context.Context, string, ...string) (string, error), target string) error {
	dir, err := run(ctx, "mktemp", "-d", "/usr/bin/.tantor-java-XXXXXXXX")
	if err != nil {
		return err
	}
	if !regexp.MustCompile(`^/usr/bin/\.tantor-java-[A-Za-z0-9]+$`).MatchString(dir) {
		return fmt.Errorf("unexpected Java link work directory")
	}
	defer func() { _, _ = run(context.WithoutCancel(ctx), "rm", "-rf", "--", dir) }()
	link := filepath.Join(dir, "java")
	if _, err = run(ctx, "ln", "-s", "--", target, link); err != nil {
		return err
	}
	_, err = run(ctx, "mv", "-Tf", "--", link, "/usr/bin/java")
	return err
}

func InspectArchive(path, version string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	var root string
	var release, binary bool
	var bytes int64
	seen := map[string]bool{}
	segment := regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.+-]*$`)
	licenseTarget := regexp.MustCompile(`^\.\./[A-Za-z0-9][A-Za-z0-9_.-]*/[A-Za-z0-9][A-Za-z0-9_.-]*$`)
	arch := map[string]string{"amd64": "x86_64", "arm64": "aarch64"}[runtime.GOARCH]
	for count := 0; ; count++ {
		h, readErr := tr.Next()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
		name := strings.TrimSuffix(strings.TrimPrefix(h.Name, "./"), "/")
		parts := strings.Split(name, "/")
		bytes += h.Size
		if count >= 10000 || h.Size < 0 || bytes > 2<<30 || seen[name] {
			return fmt.Errorf("JDK archive exceeds limits or contains duplicate entries")
		}
		seen[name] = true
		for _, part := range parts {
			if !segment.MatchString(part) {
				return fmt.Errorf("invalid JDK archive entry %q", h.Name)
			}
		}
		if root == "" {
			root = parts[0]
		}
		if parts[0] != root || (len(parts) == 1 && h.Typeflag != tar.TypeDir) || h.Mode&07000 != 0 {
			return fmt.Errorf("unexpected JDK archive entry %q", h.Name)
		}
		switch h.Typeflag {
		case tar.TypeDir, tar.TypeReg:
		case tar.TypeSymlink:
			if len(parts) != 4 || parts[1] != "legal" || !licenseTarget.MatchString(h.Linkname) {
				return fmt.Errorf("unexpected JDK archive link %q", h.Name)
			}
		default:
			return fmt.Errorf("unsupported JDK archive entry %q", h.Name)
		}
		switch strings.Join(parts[1:], "/") {
		case "release":
			if h.Typeflag != tar.TypeReg || h.Size > 16384 {
				return fmt.Errorf("invalid JDK release metadata")
			}
			data, readErr := io.ReadAll(tr)
			if readErr != nil {
				return readErr
			}
			props := map[string]string{}
			for _, line := range strings.Split(string(data), "\n") {
				if k, v, ok := strings.Cut(line, "="); ok {
					props[k] = strings.Trim(strings.TrimSpace(v), "\"")
				}
			}
			release = arch != "" && props["OS_NAME"] == "Linux" && props["OS_ARCH"] == arch && props["JAVA_VERSION"] == version
		case "bin/java":
			binary = h.Typeflag == tar.TypeReg && h.Mode&0111 != 0 && h.Size > 0
		}
	}
	if !release || !binary {
		return fmt.Errorf("JDK does not match Linux architecture, exact version or executable layout")
	}
	return nil
}

func Allowed(oldVersion, target string) bool {
	if !javaVersionPattern.MatchString(oldVersion) || !javaVersionPattern.MatchString(target) {
		return false
	}
	old, next := strings.Split(oldVersion, "."), strings.Split(target, ".")
	if old[0] == "17" && next[0] == "25" {
		return false
	}
	for i := range old {
		x, err1 := strconv.Atoi(old[i])
		y, err2 := strconv.Atoi(next[i])
		if err1 != nil || err2 != nil {
			return false
		}
		if y != x {
			return y > x
		}
	}
	return false
}

// ArchivePath accepts only an approved absolute parcel directory and artifact UUID.
func ArchivePath(base, id string) (string, error) {
	base, err := taskvalidate.ApprovedPath(base)
	if err != nil || !javaArtifactPattern.MatchString(id) {
		return "", fmt.Errorf("approved parcel directory and artifact UUID required")
	}
	return filepath.Join(base, ".downloads", "java-"+id+".tar.gz"), nil
}
