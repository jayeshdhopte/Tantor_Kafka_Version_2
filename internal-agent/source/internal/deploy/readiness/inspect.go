// Package readiness performs bounded observation-only Add Node checks.
// It must not install, stop services, create directories, write configs or format storage.
package readiness

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/internal/taskvalidate"
)

type Request struct {
	SchemaVersion                string      `json:"schemaVersion"`
	HostID                       string      `json:"hostId"`
	HostIP                       string      `json:"hostIp"`
	HostIdentityUnique           bool        `json:"hostIdentityUnique"`
	ContextHash                  string      `json:"contextHash"`
	ContextStartedAt             time.Time   `json:"contextStartedAt"`
	ExpiresAt                    time.Time   `json:"expiresAt"`
	KafkaVersion                 string      `json:"kafkaVersion"`
	KafkaClusterID               string      `json:"kafkaClusterId"`
	BrokerPort                   string      `json:"brokerPort"`
	RuntimeUser                  string      `json:"runtimeUser"`
	JavaHome                     string      `json:"javaHome"`
	DataPaths                    []string    `json:"dataPaths"`
	MetadataPath                 string      `json:"metadataPath"`
	LogPath                      string      `json:"logPath"`
	InstallPath                  string      `json:"installPath"`
	ParcelDir                    string      `json:"parcelDir"`
	ArtifactChecksum             string      `json:"artifactChecksum"`
	ControllerEndpoints          []string    `json:"controllerEndpoints"`
	BrokerEndpoints              []string    `json:"brokerEndpoints"`
	SecurityMode                 string      `json:"securityMode"`
	QuorumMode                   string      `json:"quorumMode"`
	HeapXmx                      string      `json:"heapXmx"`
	Ports                        []string    `json:"ports"`
	LimitNofile                  int         `json:"limitNofile"`
	MinAvailableMemoryMb         int         `json:"minAvailableMemoryMb"`
	MinDiskGb                    int         `json:"minDiskGb"`
	MinInodes                    int         `json:"minInodes"`
	AllowedJavaMajors            []int       `json:"allowedJavaMajors"`
	PeerProbes                   []PeerProbe `json:"peerProbes"`
	ExpectedBrokerProbeCount     int         `json:"expectedBrokerProbeCount"`
	ExpectedControllerProbeCount int         `json:"expectedControllerProbeCount"`
	ExpectedClientProbeCount     int         `json:"expectedClientProbeCount"`
}

type Check struct {
	Code       string    `json:"code"`
	Name       string    `json:"name"`
	Phase      string    `json:"phase"`
	Status     string    `json:"status"`
	Expected   any       `json:"expected"`
	Observed   any       `json:"observed"`
	Remedy     string    `json:"remedy"`
	ObservedAt time.Time `json:"observedAt"`
	Source     string    `json:"source"`
}
type Report struct {
	SchemaVersion string  `json:"schemaVersion"`
	HostID        string  `json:"hostId"`
	ContextHash   string  `json:"contextHash"`
	Checks        []Check `json:"checks"`
}

var codes = []string{"C09", "C10", "C12", "C13", "C14", "C15", "C16", "C17", "C18", "C19", "C20", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B10", "B11"}
var userPattern = regexp.MustCompile(`^[a-z_][a-z0-9_-]*[$]?$`)
var versionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(?:[.-][A-Za-z0-9]+)*$`)

func Validate(raw, hostID string) (Request, error) {
	var r Request
	if err := json.Unmarshal([]byte(raw), &r); err != nil {
		return r, fmt.Errorf("invalid readiness request")
	}
	if r.SchemaVersion != "add-node/v1" || r.HostID != hostID || len(r.ContextHash) != 64 {
		return r, fmt.Errorf("readiness identity or schema mismatch")
	}
	if r.ContextStartedAt.IsZero() || r.ContextStartedAt.After(time.Now().Add(5*time.Second)) || !r.ExpiresAt.After(time.Now()) {
		return r, fmt.Errorf("readiness request expired or clock is inconsistent")
	}
	if !versionPattern.MatchString(r.KafkaVersion) || (r.RuntimeUser != "" && !userPattern.MatchString(r.RuntimeUser)) {
		return r, fmt.Errorf("approved Kafka version and configured runtime_user required")
	}
	if net.ParseIP(r.HostIP) == nil {
		return r, fmt.Errorf("candidate IP must be explicit")
	}
	if r.MinAvailableMemoryMb < 1 || r.MinDiskGb < 1 || r.MinInodes < 1 || r.LimitNofile < 1 {
		return r, fmt.Errorf("positive admission policy required")
	}
	for _, p := range append(append(append([]string{}, r.DataPaths...), r.MetadataPath, r.LogPath, r.InstallPath), r.ParcelDir, r.JavaHome) {
		if p != "" {
			if _, err := taskvalidate.ApprovedPath(p); err != nil {
				return r, fmt.Errorf("unsafe readiness path")
			}
		}
	}
	for _, port := range r.Ports {
		if _, err := taskvalidate.Port(port); err != nil {
			return r, err
		}
	}
	if _, err := taskvalidate.Port(r.BrokerPort); err != nil {
		return r, err
	}
	if len(r.PeerProbes) > 128 || len(r.ControllerEndpoints) > 128 || len(r.BrokerEndpoints) > 128 || len(r.DataPaths) > 16 {
		return r, fmt.Errorf("inspection scope exceeds bounded limits")
	}
	for _, probe := range r.PeerProbes {
		if probe.HostID == "" || len(probe.Token) != 36 {
			return r, fmt.Errorf("invalid peer probe identity")
		}
	}
	for _, endpoint := range append(append([]string{}, r.ControllerEndpoints...), r.BrokerEndpoints...) {
		if _, err := taskvalidate.HostPort(endpoint); err != nil {
			return r, err
		}
	}
	return r, nil
}

func Inspect(ctx context.Context, r Request, hostID string, exec executor.Executor) Report {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	report := Report{SchemaVersion: "add-node/v1", HostID: hostID, ContextHash: r.ContextHash}
	probe, probeErr := listenProbe(ctx, net.JoinHostPort(r.HostIP, r.BrokerPort), r.PeerProbes)
	if probe != nil {
		defer probe.listener.Close()
	}
	checks := map[string]Check{}
	put := func(code, name, state string, expected, observed any, remedy string) {
		phase := "COMMON"
		if strings.HasPrefix(code, "B") {
			phase = "BROKER"
		}
		checks[code] = Check{code, name, phase, state, expected, observed, remedy, time.Now().UTC(), "AGENT"}
	}
	for _, code := range codes {
		put(code, "Readiness observation", "UNKNOWN", "Measured evidence", "Not measured", "Resolve missing policy or observation support and rerun.")
	}
	measure := func(code, name, expected, script string, args ...string) (string, bool) {
		if (code == "C16" || code == "C19" || code == "B10") && !userPattern.MatchString(r.RuntimeUser) {
			put(code, name, "UNKNOWN", expected, "runtime_user not configured", "Configure the inherited Kafka runtime user; this check does not create users.")
			return "", false
		}
		callCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		defer stop()
		out, _, err := exec.Run(callCtx, "bash", append([]string{"-c", script, "--"}, args...)...)
		state, remedy := "PASS", ""
		if err != nil {
			state = "FAIL"
			remedy = "Check the expected setting and agent observation permissions; no remediation was applied."
		}
		if len(out) > 1500 {
			out = out[:1500]
		}
		put(code, name, state, expected, strings.TrimSpace(out), remedy)
		return out, err == nil
	}

	identityOK := false
	if addresses, err := net.InterfaceAddrs(); err == nil {
		for _, address := range addresses {
			ip, _, err := net.ParseCIDR(address.String())
			if err == nil && ip.Equal(net.ParseIP(r.HostIP)) {
				identityOK = true
			}
		}
	}
	state := "FAIL"
	localInterface := identityOK
	identityOK = identityOK && r.HostIdentityUnique
	if identityOK {
		state = "PASS"
	}
	put("C09", "Candidate machine identity", state, "Unique enrolled host ID and intended IP on a local interface", map[string]any{"hostId": hostID, "ip": r.HostIP, "localInterface": localInterface, "inventoryUnique": r.HostIdentityUnique}, "Verify enrolled machine/address identity if mismatched.")
	put("C10", "Live inspection capability", "PASS", "add-node/v1 structured live command", "INSPECT_ADD_NODE completed on enrolled agent", "")
	measure("C12", "Existing Kafka deployment", "No Kafka unit definition or process, including stopped services", `units=$(systemctl list-unit-files --type=service --no-legend --no-pager 2>/dev/null) || exit 1
conflicts=$(printf '%s\n' "$units" | awk '{print $1}' | grep -E '(kafka|broker|controller)(-|\.|@)' | grep -vE '^dbus-broker\.service$|exporter|monitoring|metrics' || true)
processes=$(pgrep -f '[k]afka[.]Kafka|[K]afkaRaftServer' || true)
[ -z "$conflicts" ] && [ -z "$processes" ] || { echo 'Existing Kafka service or process detected'; exit 1; }
echo 'No matching Kafka units or processes'`)
	paths := append([]string{}, r.DataPaths...)
	if r.MetadataPath != "" {
		paths = append(paths, r.MetadataPath)
	}
	storageOK := len(paths) > 0
	for _, p := range paths {
		if err := emptyStorage(p); err != nil {
			storageOK = false
		}
	}
	if storageOK {
		state = "PASS"
	} else {
		state = "FAIL"
	}
	put("C13", "Existing storage identity", state, "All intended data/metadata paths absent or empty, no symlinks", map[string]any{"paths": paths, "empty": storageOK}, "Inspect conflicting data manually; this preview never deletes or reformats it.")
	put("B02", "Broker storage layout", state, "Distinct validated broker data paths and effective metadata path", paths, "Resolve storage conflicts and mount policy before proceeding.")

	archive := findArchive(r)
	if archive == "" {
		put("C14", "Host Kafka package", "FAIL", "Approved distribution archive present on target", "Matching cached archive not found", "Explicitly distribute the matching Kafka package through Artifacts, then rerun checks.")
	} else {
		put("C14", "Host Kafka package", "PASS", "Approved distribution archive present on target", archive, "")
	}
	if archive != "" && regexp.MustCompile(`^[a-fA-F0-9]{64}$`).MatchString(r.ArtifactChecksum) {
		hash, err := hashArchive(ctx, archive)
		status := "FAIL"
		if err == nil && strings.EqualFold(hash, r.ArtifactChecksum) {
			status = "PASS"
		}
		put("C15", "Package SHA-256", status, r.ArtifactChecksum, hash, "Redistribute the approved package if the checksum differs; no package was executed by this check.")
	}

	java := "java"
	if r.JavaHome != "" {
		java = filepath.Join(r.JavaHome, "bin", "java")
	}
	javaOut, javaOK := measure("C16", "Runtime user's Java", "Explicit supported Java major from policy", `runuser -u "$1" -- "$2" -version 2>&1`, r.RuntimeUser, java)
	if javaOK {
		major := javaMajor(javaOut)
		allowed := false
		for _, value := range r.AllowedJavaMajors {
			if major == value {
				allowed = true
			}
		}
		if !allowed {
			put("C16", "Runtime user's Java", "FAIL", r.AllowedJavaMajors, major, "Configure the service user's Java to match the approved package policy.")
		}
	}
	measure("C17", "OS and service runtime", "Linux amd64/arm64, systemd, synchronized clock and kernel nofile ceiling", `uname -sm
arch=$(uname -m); [ "$arch" = x86_64 ] || [ "$arch" = aarch64 ] || exit 1
[ "$(uname -s)" = Linux ] || exit 1
systemctl --version | head -n1
[ "$(timedatectl show -p NTPSynchronized --value)" = yes ] || exit 1
[ "$(cat /proc/sys/fs/nr_open)" -ge "$1" ]`, strconv.Itoa(r.LimitNofile))
	_, resourceOK := measure("C18", "Available host resources", fmt.Sprintf("Available RAM >= %d MiB and load <= logical CPU count", r.MinAvailableMemoryMb), `free_kb=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
load=$(cut -d' ' -f1 /proc/loadavg); cpus=$(getconf _NPROCESSORS_ONLN)
printf 'MemAvailableKiB=%s load1=%s cpus=%s\n' "$free_kb" "$load" "$cpus"
[ -n "$free_kb" ] && [ "$free_kb" -ge "$1" ] && awk -v l="$load" -v c="$cpus" 'BEGIN { exit !(c>0 && l<=c) }'`, strconv.Itoa(r.MinAvailableMemoryMb*1024))
	_, mountsOK := measure("C19", "Persistent mounts and access", "Explicit non-root persistent mount; free space/inodes; service-user access", `[ "$#" -ge 5 ] || exit 1
user=$1; min_kb=$2; min_inodes=$3; shift 3
for path in "$@"; do
  parent=$path; while [ ! -e "$parent" ] && [ "$parent" != / ]; do parent=$(dirname -- "$parent"); done
  target=$(findmnt -n -o TARGET -T "$parent") || exit 1
  fstype=$(findmnt -n -o FSTYPE -T "$parent") || exit 1
  options=$(findmnt -n -o OPTIONS -T "$parent") || exit 1
  printf '%s: mount=%s type=%s\n' "$path" "$target" "$fstype"
  [ -n "$target" ] && [ "$target" != / ] || exit 1
  case "$fstype" in tmpfs|ramfs|overlay) exit 1;; esac
  case ",$options," in *,ro,*) exit 1;; esac
  [ "$(df -Pk -- "$parent" | tail -n1 | awk '{print $4}')" -ge "$min_kb" ] || exit 1
  [ "$(df -Pi -- "$parent" | tail -n1 | awk '{print $4}')" -ge "$min_inodes" ] || exit 1
  runuser -u "$user" -- test -w "$parent" || exit 1
  runuser -u "$user" -- test -x "$parent" || exit 1
done`, append([]string{r.RuntimeUser, strconv.Itoa(r.MinDiskGb * 1024 * 1024), strconv.Itoa(r.MinInodes)}, append(paths, r.LogPath)...)...)
	if resourceOK && mountsOK && heapBytes(r.HeapXmx) > 0 && heapBytes(r.HeapXmx) <= int64(r.MinAvailableMemoryMb)*1024*1024/2 {
		put("B03", "Broker memory/storage profile", "PASS", "Configured heap <= half the minimum available-memory admission threshold", r.HeapXmx, "")
	} else {
		put("B03", "Broker memory/storage profile", "UNKNOWN", "Explicit heap, sufficient memory and persistent storage", r.HeapXmx, "Set a supported heap profile and resolve memory/mount checks; workload throughput sizing still needs a deployment profile.")
	}

	configOK := r.SecurityMode == "PLAINTEXT" && (r.QuorumMode == "STATIC" || r.QuorumMode == "DYNAMIC") && len(r.ControllerEndpoints) > 0 && identityOK
	if configOK {
		state = "PASS"
	} else {
		state = "FAIL"
	}
	put("B04", "Inherited broker listener inputs", state, "Broker-only; PLAINTEXT; explicit local address/port and controller discovery", map[string]any{"address": r.HostIP, "port": r.BrokerPort, "securityMode": r.SecurityMode, "quorumMode": r.QuorumMode}, "Resolve inherited listener inputs; secured clusters are not supported by this preview adapter.")
	portsFree := probeErr == nil
	listeners := []net.Listener{}
	for _, port := range r.Ports {
		if port == r.BrokerPort {
			continue
		}
		listener, err := net.Listen("tcp", net.JoinHostPort("", port))
		if err != nil {
			portsFree = false
		} else {
			listeners = append(listeners, listener)
		}
	}
	for _, listener := range listeners {
		_ = listener.Close()
	}
	if portsFree && len(r.Ports) > 0 {
		state = "PASS"
	} else {
		state = "FAIL"
	}
	put("B05", "Broker and auxiliary ports", state, "All planned TCP ports free", r.Ports, "Resolve port conflicts; no service was stopped.")
	connected := probeEndpoints(ctx, append(append([]string{}, r.BrokerEndpoints...), r.ControllerEndpoints...))
	if connected && len(r.ControllerEndpoints) > 0 && len(r.BrokerEndpoints) > 0 {
		state = "PASS"
	} else {
		state = "FAIL"
	}
	put("C20", "Outgoing Kafka network paths", state, "Candidate can resolve/connect to every inherited broker/controller endpoint", map[string]any{"brokers": r.BrokerEndpoints, "controllers": r.ControllerEndpoints, "connected": connected}, "Check DNS, routing and firewall paths from the candidate.")
	probe.wait(ctx)
	for _, test := range []struct {
		code, name, role string
		expected         int
	}{
		{"B06", "Advertised client-network route", "CLIENT", r.ExpectedClientProbeCount},
		{"B07", "Peer broker network routes", "BROKER", r.ExpectedBrokerProbeCount},
		{"B08", "Controller-to-broker routes", "CONTROLLER", r.ExpectedControllerProbeCount},
	} {
		state := "FAIL"
		count := probe.count(test.role)
		if test.expected == 0 {
			state = "UNKNOWN"
		} else if count == test.expected && identityOK && (test.role == "CLIENT" || connected) {
			state = "PASS"
		}
		put(test.code, test.name, state, fmt.Sprintf("Nonce-bound probes from %d configured %s host(s)", test.expected, test.role), count,
			"Configure enrolled client probe hosts and keep all required peer agents online; resolve routing/firewall failures. This is a route probe, not post-start Kafka verification.")
	}
	if archive != "" && checks["C15"].Status == "PASS" && javaOK && r.ParcelDir != "" {
		installed := filepath.Join(r.ParcelDir, "kafka", r.KafkaVersion)
		if err := verifyInstalledArchive(ctx, archive, installed); err != nil {
			put("B10", "Installed package content", "FAIL", "Installed files match the approved archive before CLI execution", "Package content mismatch or unverified extraction", "Redistribute the approved package; no installed script was executed.")
		} else {
			jars, _ := filepath.Glob(filepath.Join(installed, "libs", "kafka_*-"+r.KafkaVersion+".jar"))
			state := "FAIL"
			if len(jars) == 1 {
				if runtimeParcelAccess(ctx, installed, jars[0], r.RuntimeUser, exec) == nil {
					state = "PASS"
				}
			}
			put("B10", "Installed approved package identity", state, "Exact approved payload and runtime-user read/execute access", map[string]any{"version": r.KafkaVersion, "matchingCoreJars": len(jars), "payloadVerified": true, "runtimeAccessible": state == "PASS"}, "Verify runtime-user traversal/read/execute access to the approved parcel, or redistribute the matching bundle; final generated-config validation remains required before deployment.")
		}
	}
	if storageOK && mountsOK && r.KafkaClusterID != "" && configOK {
		put("B11", "Existing-cluster formatting inputs", "PASS", "Existing Kafka ID; empty persistent storage; no format command in preview", map[string]any{"kafkaClusterId": r.KafkaClusterID, "storagePaths": paths, "policy": "EMPTY_ONLY"}, "")
	}
	for _, code := range codes {
		report.Checks = append(report.Checks, checks[code])
	}
	return report
}

func runtimeParcelAccess(ctx context.Context, root, jar, user string, exec executor.Executor) error {
	_, _, err := exec.Run(ctx, "runuser", "-u", user, "--", "bash", "-c", `test -x "$1/bin/kafka-storage.sh" && test -x "$1/bin/kafka-run-class.sh" && test -x "$1/bin/kafka-server-start.sh" && test -r "$2"`, "--", root, jar)
	return err
}
func emptyStorage(path string) error {
	if path == "" || filepath.Clean(path) == string(filepath.Separator) {
		return fmt.Errorf("missing or root storage path")
	}
	// Reject symlink ancestors too, including a missing leaf beneath a symlinked parent.
	for parent := path; ; parent = filepath.Dir(parent) {
		info, err := os.Lstat(parent)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if err == nil && info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in storage path")
		}
		if parent == filepath.Dir(parent) {
			break
		}
	}
	entries, err := os.ReadDir(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("nonempty storage")
	}
	return nil
}
func findArchive(r Request) string {
	// The catalog-bound parcel is authoritative; a legacy install cache may hold
	// a different archive with the same Kafka version.
	for _, root := range []string{r.ParcelDir, r.InstallPath} {
		if root == "" {
			continue
		}
		for depth := 0; depth < 5 && root != filepath.Dir(root); depth++ {
			candidate := filepath.Join(root, ".downloads", "parcel_kafka_"+r.KafkaVersion+".tgz")
			if info, err := os.Lstat(candidate); err == nil && info.Mode().IsRegular() {
				return candidate
			}
			root = filepath.Dir(root)
		}
	}
	return ""
}
func hashArchive(ctx context.Context, path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	buf := make([]byte, 1024*1024)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, err := f.Read(buf)
		if n > 0 {
			_, _ = h.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func probeEndpoints(ctx context.Context, endpoints []string) bool {
	if len(endpoints) == 0 {
		return false
	}
	for _, endpoint := range endpoints {
		dialer := net.Dialer{Timeout: 2 * time.Second}
		conn, err := dialer.DialContext(ctx, "tcp", endpoint)
		if err != nil {
			return false
		}
		_ = conn.Close()
	}
	return true
}
func javaMajor(out string) int {
	match := regexp.MustCompile(`(?:version\s+"?|openjdk\s+)([0-9]+)`).FindStringSubmatch(out)
	if len(match) < 2 {
		return 0
	}
	major, _ := strconv.Atoi(match[1])
	return major
}
func heapBytes(raw string) int64 {
	match := regexp.MustCompile(`(?i)^([1-9][0-9]*)([mg])$`).FindStringSubmatch(raw)
	if len(match) < 3 {
		return 0
	}
	size, _ := strconv.ParseInt(match[1], 10, 64)
	factor := int64(1024 * 1024)
	if strings.EqualFold(match[2], "g") {
		factor *= 1024
	}
	return size * factor
}

// Compare archive payloads without extraction or execution. Names follow the parcel
// installer's strip-components=1 layout; links/escapes are rejected, not followed.
func verifyInstalledArchive(ctx context.Context, archive, installed string) error {
	installed = filepath.Clean(installed)
	seen := map[string]bool{}
	f, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	reader := tar.NewReader(gz)
	count := 0
	var bytes int64
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if header.Typeflag == tar.TypeDir {
			continue
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
			return fmt.Errorf("unsupported archive link/type")
		}
		parts := strings.SplitN(header.Name, "/", 2)
		if len(parts) != 2 || strings.HasPrefix(header.Name, "/") {
			return fmt.Errorf("unexpected archive layout")
		}
		rel := path.Clean(parts[1])
		if rel == "." || rel == ".." || strings.HasPrefix(rel, "../") || strings.Contains(rel, "\\") {
			return fmt.Errorf("unsafe archive member")
		}
		count++
		bytes += header.Size
		if count > 100000 || header.Size < 0 || bytes > 4*1024*1024*1024 {
			return fmt.Errorf("archive exceeds inspection bounds")
		}
		target := filepath.Join(installed, filepath.FromSlash(rel))
		seen[target] = true
		for p := target; ; p = filepath.Dir(p) {
			info, err := os.Lstat(p)
			if err != nil {
				return err
			}
			if info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("symlink in installed content")
			}
			if p == installed {
				break
			}
			if p == filepath.Dir(p) {
				return fmt.Errorf("invalid install root")
			}
		}
		installedFile, err := os.Open(target)
		if err != nil {
			return err
		}
		a, b := sha256.New(), sha256.New()
		_, archiveErr := io.Copy(a, reader)
		n, installedErr := io.Copy(b, io.LimitReader(installedFile, header.Size+1))
		_ = installedFile.Close()
		if archiveErr != nil || installedErr != nil || n != header.Size || !strings.EqualFold(hex.EncodeToString(a.Sum(nil)), hex.EncodeToString(b.Sum(nil))) {
			return fmt.Errorf("installed payload differs")
		}
	}
	if count == 0 {
		return fmt.Errorf("empty package")
	}
	return filepath.WalkDir(installed, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !d.IsDir() && !seen[p] {
			return fmt.Errorf("unapproved extra installed file")
		}
		return nil
	})
}
