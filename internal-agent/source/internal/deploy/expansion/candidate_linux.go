//go:build linux

package expansion

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	osuser "os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"io.translab/tantor-agent/internal/deploy/kafka"
	"io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/pkg/logrotation"
)

func RunCandidate(ctx context.Context, hostID, base string, params map[string]string, e executor.Executor, download func(string, string) (string, error)) (string, error) {
	if os.Geteuid() != 0 {
		return "", fmt.Errorf("candidate expansion requires a root Linux internal agent")
	}
	p, err := Decode(params["expansion_plan"], params["plan_digest"], hostID)
	if err != nil {
		return "", err
	}
	bounded, deadlineCancel, err := dispatchContext(ctx, params)
	if err != nil {
		return "", err
	}
	defer deadlineCancel()
	ctx = bounded
	if hostID != p.CandidateHostID || !safePath(base) {
		return "", fmt.Errorf("candidate host/runtime scope mismatch")
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
	dir := filepath.Join(root, p.OperationID)
	if err = privateDirectory(dir); err != nil {
		return "", err
	}
	env := &nativeCandidateEnvironment{dir: dir, exec: e, download: download}
	if params["action"] == "VALIDATE" {
		if err = env.ValidateArtifact(ctx, p); err != nil {
			return "", err
		}
		for _, process := range p.Processes {
			if err = env.ValidateFresh(ctx, p, process); err != nil {
				return "", err
			}
		}
		raw, err := json.Marshal(map[string]any{"schemaVersion": "add-quorum-capability/v1", "operationId": p.OperationID, "hostId": hostID, "planDigest": params["plan_digest"], "contextHash": p.ContextHash, "payloadVerified": true, "candidateReady": true, "rootMutationSupported": true, "observedAt": time.Now().UTC(), "supportedCommands": []string{"EXECUTE_QUORUM_CANDIDATE", "EXECUTE_QUORUM_PARTICIPANT", "VALIDATE_QUORUM_EXPANSION"}})
		return string(raw), err
	}
	var proof *ControllerProof
	if raw := params["controller_proof"]; raw != "" {
		if len(raw) > 16384 || json.Unmarshal([]byte(raw), &proof) != nil {
			return "", fmt.Errorf("invalid controller proof")
		}
	}
	report, err := executeCandidate(ctx, p, params["plan_digest"], hostID, params["role"], params["action"], proof, env)
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(report)
	return string(raw), err
}

type nativeCandidateEnvironment struct {
	dir      string
	exec     executor.Executor
	download func(string, string) (string, error)
	manifest *ArtifactManifest
}

func (e *nativeCandidateEnvironment) LoadReceipt(role string) (*CandidateReceipt, error) {
	if role != "controller" && role != "broker" {
		return nil, fmt.Errorf("unsupported role")
	}
	raw, err := readBounded(context.Background(), filepath.Join(e.dir, role+".json"), 32768)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var r CandidateReceipt
	if json.Unmarshal([]byte(raw), &r) != nil {
		return nil, fmt.Errorf("invalid candidate ownership receipt")
	}
	return &r, nil
}
func (e *nativeCandidateEnvironment) SaveReceipt(r CandidateReceipt) error {
	if r.Role != "controller" && r.Role != "broker" {
		return fmt.Errorf("unsupported receipt role")
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return durableReplace(filepath.Join(e.dir, r.Role+".json"), string(raw), 0600, nil)
}
func (e *nativeCandidateEnvironment) ValidateArtifact(ctx context.Context, p Plan) error {
	m, err := loadManifest(ctx, p)
	if err != nil {
		return err
	}
	home := filepath.Join(p.ParcelDir, "kafka", p.KafkaVersion)
	if err = verifyRuntimePayload(ctx, m, home); err != nil {
		return err
	}
	e.manifest = &m
	return nil
}
func (e *nativeCandidateEnvironment) ValidateFresh(ctx context.Context, p Plan, c Process) error {
	if _, err := osuser.Lookup(p.RuntimeUser); err != nil {
		return fmt.Errorf("reviewed runtime user does not exist")
	}
	if err := noLinks(filepath.Join(p.JavaHome, "bin", "java")); err != nil {
		return err
	}
	out, stderr, err := e.exec.Run(ctx, filepath.Join(p.JavaHome, "bin", "java"), "-version")
	if err != nil {
		return fmt.Errorf("reviewed Java runtime is unavailable")
	}
	versionText := out + "\n" + stderr
	major := javaMajor.FindStringSubmatch(versionText)
	if len(major) != 2 {
		return fmt.Errorf("reviewed Java runtime version is unavailable")
	}
	majorNumber, _ := strconv.Atoi(major[1])
	allowedJava := false
	for _, allowed := range p.ResourcePolicy.AllowedJavaMajors {
		allowedJava = allowedJava || allowed == majorNumber
	}
	if !allowedJava {
		return fmt.Errorf("candidate Java major differs from the reviewed admission policy")
	}
	if err := verifyResources(ctx, p); err != nil {
		return err
	}
	// Every planned listener is accounted for, including the sibling process.
	for _, process := range p.Processes {
		for _, port := range []int{process.Port, process.Runtime.JmxPort} {
			if port == 0 {
				continue
			}
			listener, err := net.Listen("tcp", net.JoinHostPort(p.CandidateIP, strconv.Itoa(port)))
			if err != nil {
				return fmt.Errorf("candidate listener is not locally available")
			}
			listener.Close()
		}
	}
	for _, d := range storagePaths(c) {
		if err := emptyDirectory(d); err != nil {
			return err
		}
	}
	if err := emptyDirectory(c.LogPath); err != nil {
		return err
	}
	unitPath := filepath.Join("/etc/systemd/system", p.Unit(c.Role))
	if info, err := os.Lstat(unitPath); err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("candidate unit is not a regular owned file")
		}
		raw, err := readBounded(ctx, unitPath, 262144)
		if err != nil || raw != candidateUnit(p, c) {
			return fmt.Errorf("candidate unit already exists outside this operation")
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	// Reject any running Kafka JVM on a supposedly fresh candidate host. This
	// includes processes started outside systemd; their command lines stay local.
	entries, err := os.ReadDir("/proc")
	if err != nil || len(entries) > 65536 {
		return fmt.Errorf("candidate process inventory unavailable")
	}
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		raw, err := readBounded(ctx, filepath.Join("/proc", entry.Name(), "cmdline"), 65536)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("candidate process inventory is incomplete")
		}
		for _, arg := range strings.Split(raw, "\x00") {
			if arg == "kafka.Kafka" {
				return fmt.Errorf("candidate already has a running Kafka process")
			}
		}
	}
	return nil
}
func (e *nativeCandidateEnvironment) Observe(ctx context.Context, p Plan, c Process) (CandidateObservation, error) {
	o := CandidateObservation{DirectoryIDs: map[string]string{}}
	properties, err := p.Properties(c.Role)
	if err != nil {
		return o, err
	}
	raw, err := readBounded(ctx, p.ConfigPath(c.Role), 262144)
	o.ConfigOwned = err == nil && raw == properties
	unitPath := filepath.Join("/etc/systemd/system", p.Unit(c.Role))
	raw, err = readBounded(ctx, unitPath, 262144)
	o.UnitOwned = err == nil && raw == candidateUnit(p, c)
	configDir := filepath.Dir(p.ConfigPath(c.Role))
	raw, err = readBounded(ctx, filepath.Join(configDir, c.Role+"-log4j2.xml"), 262144)
	o.ConfigOwned = o.ConfigOwned && err == nil && raw == logrotation.KafkaLog4j2(c.LogPath, p.Unit(c.Role)+".log")
	if c.Runtime.JmxPort > 0 {
		sum, err := fileSHA256(ctx, filepath.Join(configDir, c.Role+"-jmx.jar"), 128*1024*1024)
		raw, readErr := readBounded(ctx, filepath.Join(configDir, c.Role+"-jmx.yml"), 262144)
		o.ConfigOwned = o.ConfigOwned && err == nil && sum == c.Runtime.JmxChecksum && readErr == nil && raw == kafka.JmxConfigTemplate
	}
	matching, missing := 0, 0
	seen := map[string]bool{}
	for _, d := range storagePaths(c) {
		raw, err := readBounded(ctx, filepath.Join(d, "meta.properties"), 16384)
		if os.IsNotExist(err) {
			if emptyDirectory(d) != nil {
				o.StorageState = "PARTIAL"
				return o, nil
			}
			missing++
			continue
		}
		if err != nil {
			o.StorageState = "UNKNOWN"
			return o, nil
		}
		props, err := simpleProperties(raw)
		id := props["directory.id"]
		if err != nil || props["cluster.id"] != p.KafkaClusterID || props["node.id"] != strconv.Itoa(c.NodeID) || !kafkaID.MatchString(id) || id == "AAAAAAAAAAAAAAAAAAAAAA" || seen[id] {
			o.StorageState = "FOREIGN"
			return o, nil
		}
		seen[id] = true
		o.DirectoryIDs[d] = id
		matching++
		if d == metadataPath(c) {
			o.MetadataDirectoryID = id
		}
	}
	switch {
	case matching == len(storagePaths(c)):
		o.StorageState = "MATCHING"
	case missing == len(storagePaths(c)):
		o.StorageState = "MISSING"
	default:
		o.StorageState = "PARTIAL"
	}
	// A freshly written unit may not be loaded until START performs daemon-reload.
	inspector := &nativeParticipantEnvironment{exec: e.exec}
	state, showErr := inspector.show(ctx, p.Unit(c.Role))
	if showErr != nil {
		if o.UnitOwned {
			out, _, err := e.exec.Run(ctx, "systemctl", "show", "--no-pager", "--property=LoadState", p.Unit(c.Role))
			if err == nil && strings.TrimSpace(out) == "LoadState=not-found" {
				return o, nil
			}
		}
		if !o.UnitOwned {
			return o, nil
		}
		return o, showErr
	}
	o.ServiceActive = state["ActiveState"] == "active" && state["SubState"] == "running"
	if !o.ServiceActive {
		return o, nil
	}
	pid, err := strconv.Atoi(state["MainPID"])
	if err != nil || pid < 1 {
		return o, fmt.Errorf("active candidate PID unavailable")
	}
	proc := filepath.Join("/proc", strconv.Itoa(pid))
	cmd, err := readBounded(ctx, filepath.Join(proc, "cmdline"), 65536)
	if err != nil {
		return o, err
	}
	stat, err := readBounded(ctx, filepath.Join(proc, "stat"), 16384)
	if err != nil {
		return o, err
	}
	ticks := processStartTicks(stat)
	if ticks == "" {
		return o, fmt.Errorf("candidate process identity unavailable")
	}
	if e.manifest == nil {
		m, err := loadManifest(ctx, p)
		if err != nil {
			return o, err
		}
		e.manifest = &m
	}
	agents := map[string]string{}
	if c.Runtime.JmxPort > 0 {
		agents[filepath.Join(configDir, c.Role+"-jmx.jar")] = c.Runtime.JmxChecksum
	}
	_, err = VerifyRuntime(ctx, *e.manifest, filepath.Join(p.ParcelDir, "kafka", p.KafkaVersion), p.ConfigPath(c.Role), strings.Split(strings.TrimSuffix(cmd, "\x00"), "\x00"), agents, diskRuntimeFiles{})
	if err != nil {
		return o, err
	}
	again, err := inspector.show(ctx, p.Unit(c.Role))
	if err != nil || again["MainPID"] != state["MainPID"] || again["ExecMainStartTimestampMonotonic"] != state["ExecMainStartTimestampMonotonic"] || again["ActiveState"] != "active" {
		return o, fmt.Errorf("candidate process changed during observation")
	}
	againCmd, err := readBounded(ctx, filepath.Join(proc, "cmdline"), 65536)
	if err != nil || againCmd != cmd {
		return o, fmt.Errorf("candidate command changed during observation")
	}
	againStat, err := readBounded(ctx, filepath.Join(proc, "stat"), 16384)
	if err != nil || processStartTicks(againStat) != ticks {
		return o, fmt.Errorf("candidate PID was reused during observation")
	}
	o.RuntimeVerified = true
	return o, nil
}
func (e *nativeCandidateEnvironment) Prepare(ctx context.Context, p Plan, c Process) error {
	account, err := osuser.Lookup(p.RuntimeUser)
	if err != nil {
		return err
	}
	uid, err := strconv.Atoi(account.Uid)
	if err != nil {
		return err
	}
	gid, err := strconv.Atoi(account.Gid)
	if err != nil {
		return err
	}
	for _, d := range append(storagePaths(c), c.LogPath) {
		if err := ownedEmptyDirectory(d, uid, gid); err != nil {
			return err
		}
	}
	configDir := filepath.Dir(p.ConfigPath(c.Role))
	if err := configDirectory(configDir); err != nil {
		return err
	}
	if err := writeExclusive(filepath.Join(configDir, c.Role+"-log4j2.xml"), logrotation.KafkaLog4j2(c.LogPath, p.Unit(c.Role)+".log"), 0644); err != nil {
		return err
	}
	if c.Runtime.JmxPort > 0 {
		jar := filepath.Join(configDir, c.Role+"-jmx.jar")
		if _, err := os.Lstat(jar); os.IsNotExist(err) {
			if e.download == nil {
				return fmt.Errorf("approved JMX artifact downloader unavailable")
			}
			if _, err = e.download(c.Runtime.JmxArtifactID, jar); err != nil {
				return err
			}
		}
		sum, err := fileSHA256(ctx, jar, 128*1024*1024)
		if err != nil || sum != c.Runtime.JmxChecksum {
			return fmt.Errorf("approved JMX exporter checksum mismatch")
		}
		if err := os.Chmod(jar, 0644); err != nil {
			return err
		}
		if err := writeExclusive(filepath.Join(configDir, c.Role+"-jmx.yml"), kafka.JmxConfigTemplate, 0644); err != nil {
			return err
		}
	}
	properties, err := p.Properties(c.Role)
	if err != nil {
		return err
	}
	if err = writeExclusive(p.ConfigPath(c.Role), properties, 0644); err != nil {
		return err
	}
	return writeExclusive(filepath.Join("/etc/systemd/system", p.Unit(c.Role)), candidateUnit(p, c), 0644)
}
func (e *nativeCandidateEnvironment) Format(ctx context.Context, p Plan, c Process) error {
	for _, d := range storagePaths(c) {
		if err := emptyDirectory(d); err != nil {
			return err
		}
	}
	return runKafkaCLI(ctx, p, c, "kafka-storage.sh", formatArguments(p, c, p.ConfigPath(c.Role))...)
}
func formatArguments(p Plan, c Process, config string) []string {
	args := []string{"format", "--cluster-id", p.KafkaClusterID, "--config", config}
	if p.QuorumMode == "DYNAMIC" && c.Role == "controller" {
		args = append(args, "--no-initial-controllers")
	}
	return args
}
func (e *nativeCandidateEnvironment) Start(ctx context.Context, p Plan, c Process) error {
	if _, _, err := e.exec.Run(ctx, "systemctl", "daemon-reload"); err != nil {
		return err
	}
	_, _, err := e.exec.Run(ctx, "systemctl", "enable", "--now", p.Unit(c.Role))
	return err
}
func (e *nativeCandidateEnvironment) Admit(ctx context.Context, p Plan, c Process) error {
	return runKafkaCLI(ctx, p, c, "kafka-metadata-quorum.sh", "--bootstrap-controller", strings.Join(p.ExistingControllers(), ","), "--command-config", p.ConfigPath(c.Role), "add-controller")
}
func storagePaths(c Process) []string {
	out := append([]string{}, c.DataPaths...)
	for _, p := range out {
		if p == c.MetadataPath {
			return out
		}
	}
	if c.MetadataPath != "" {
		out = append(out, c.MetadataPath)
	}
	return out
}
func metadataPath(c Process) string {
	if c.MetadataPath != "" {
		return c.MetadataPath
	}
	return c.DataPaths[0]
}
func emptyDirectory(dir string) error {
	if err := noLinks(dir); os.IsNotExist(err) {
		parent := filepath.Dir(dir)
		for {
			if err = noLinks(parent); err == nil {
				return nil
			} else if !os.IsNotExist(err) {
				return err
			}
			if parent == filepath.Dir(parent) {
				return fmt.Errorf("storage has no existing ancestor")
			}
			parent = filepath.Dir(parent)
		}
	} else if err != nil {
		return err
	}
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.IsDir() {
		return fmt.Errorf("candidate storage is not a directory")
	}
	entries, err := f.Readdirnames(1)
	if err != nil && err != io.EOF {
		return err
	}
	if len(entries) != 0 {
		return fmt.Errorf("candidate storage or log directory is not empty")
	}
	return nil
}
func ownedEmptyDirectory(dir string, uid, gid int) error {
	if err := emptyDirectory(dir); err != nil {
		return err
	}
	if err := configDirectory(filepath.Dir(dir)); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0750); err != nil && !os.IsExist(err) {
		return err
	}
	if err := noLinks(dir); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0750); err != nil {
		return err
	}
	return os.Chown(dir, uid, gid)
}
func configDirectory(dir string) error {
	if err := noLinks(dir); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	parent := filepath.Dir(dir)
	if parent == dir {
		return fmt.Errorf("missing configuration root")
	}
	if err := configDirectory(parent); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0755); err != nil && !os.IsExist(err) {
		return err
	}
	return noLinks(dir)
}
func candidateUnit(p Plan, c Process) string {
	config := p.ConfigPath(c.Role)
	configDir := filepath.Dir(config)
	jmx := ""
	if c.Runtime.JmxPort > 0 {
		jmx = fmt.Sprintf("Environment=KAFKA_OPTS=-javaagent:%s/%s-jmx.jar=%d:%s/%s-jmx.yml\n", configDir, c.Role, c.Runtime.JmxPort, configDir, c.Role)
	}
	return fmt.Sprintf("# Tantor quorum expansion %s\n[Unit]\nDescription=Tantor Kafka %s %d\nAfter=network-online.target\n[Service]\nType=simple\nUser=%s\nEnvironment=JAVA_HOME=%s\nEnvironment=KAFKA_HEAP_OPTS=-Xmx%s\nEnvironment=LOG_DIR=%s\nEnvironment=CLASSPATH=\nEnvironment=JAVA_TOOL_OPTIONS=\nEnvironment=JDK_JAVA_OPTIONS=\nEnvironment=KAFKA_OPTS=\nEnvironment=\"KAFKA_LOG4J_OPTS=-Dlog4j2.configurationFile=%s/%s-log4j2.xml -Dkafka.logs.dir=%s\"\n%sExecStart=%s/bin/kafka-server-start.sh %s\nRestart=no\nLimitNOFILE=%d\n[Install]\nWantedBy=multi-user.target\n", p.OperationID, c.Role, c.NodeID, p.RuntimeUser, p.JavaHome, c.HeapXmx, c.LogPath, configDir, c.Role, c.LogPath, jmx, filepath.Join(p.ParcelDir, "kafka", p.KafkaVersion), config, c.Runtime.LimitNofile)
}

// Killing the full runuser/bash/Java process group prevents a timed-out CLI
// from mutating storage or quorum membership after its task has completed.
func runKafkaCLI(ctx context.Context, p Plan, c Process, script string, args ...string) error {
	configDir := filepath.Dir(p.ConfigPath(c.Role))
	commandArgs := []string{"-u", p.RuntimeUser, "--", "env", "-i", "PATH=/usr/bin:/bin", "JAVA_HOME=" + p.JavaHome, "KAFKA_HEAP_OPTS=-Xmx" + c.HeapXmx, "LOG_DIR=" + c.LogPath, "KAFKA_LOG4J_OPTS=-Dlog4j2.configurationFile=" + filepath.Join(configDir, c.Role+"-log4j2.xml") + " -Dkafka.logs.dir=" + c.LogPath, "/bin/bash", filepath.Join(p.ParcelDir, "kafka", p.KafkaVersion, "bin", script)}
	commandArgs = append(commandArgs, args...)
	return runProcessGroup(ctx, "/usr/sbin/runuser", commandArgs...)
}
func runProcessGroup(ctx context.Context, name string, args ...string) error {
	command := exec.CommandContext(ctx, name, args...)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		if command.Process == nil {
			return nil
		}
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if err == syscall.ESRCH {
			return os.ErrProcessDone
		}
		return err
	}
	command.WaitDelay = 5 * time.Second
	var output cappedOutput
	command.Stdout = &output
	command.Stderr = &output
	command.Env = []string{"PATH=/usr/sbin:/usr/bin:/bin"}
	if err := command.Run(); err != nil {
		return fmt.Errorf("bounded Kafka CLI failed; reconcile native receipt and live quorum before retry")
	}
	return nil
}

type cappedOutput struct{ buffer bytes.Buffer }

func (c *cappedOutput) Write(raw []byte) (int, error) {
	n := len(raw)
	remaining := 32768 - c.buffer.Len()
	if remaining > 0 {
		if len(raw) > remaining {
			raw = raw[:remaining]
		}
		c.buffer.Write(raw)
	}
	return n, nil
}
