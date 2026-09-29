package dataservice

import (
	"context"
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/pkg/api"
	"io.translab/tantor-agent/pkg/checksum"
)

type Downloader interface {
	DownloadArtifact(string, string) (string, error)
}

func Deploy(ctx context.Context, cfg *config.Config, download Downloader, exec executor.Executor, kind string, task *api.Task) (string, error) {
	s, err := Parse(kind, task)
	if err != nil {
		return "", err
	}
	if s.Deferred {
		return "", fmt.Errorf("installation cannot defer Kafka connectivity checks")
	}
	if len(task.Checksum) != 64 || task.ArtifactURL == "" {
		return "", fmt.Errorf("artifact URL and pinned SHA-256 are required")
	}
	var logs strings.Builder
	run := func(command string, args ...string) error {
		out, errOut, e := exec.RunSudo(ctx, command, args...)
		if e != nil {
			return fmt.Errorf("%s failed: %w: %s %s", command, e, out, errOut)
		}
		return nil
	}
	// Validate resolved paths before any privileged filesystem mutation.
	if err = CheckPaths(ctx, exec, s); err != nil {
		return logs.String(), err
	}
	for _, tree := range []string{"bin", "libs", "share"} {
		_, _, e := exec.RunSudo(ctx, "bash", "-c", `test ! -e "$1" && test ! -L "$1"`, "--", s.Install+"/"+tree)
		if e != nil {
			return reconfigurePorts(ctx, exec, s, task)
		}
	}
	// Never overwrite or stop an already running service during an installation retry.
	if _, _, activeErr := exec.Run(ctx, "systemctl", "is-active", "--quiet", kind+".service"); activeErr == nil {
		return logs.String(), fmt.Errorf("%s is already active; verify the existing deployment or use an explicit service upgrade procedure", kind)
	}
	if err = SafePath(cfg.Paths.ArtifactsDir); err != nil {
		return "", err
	}
	for _, dir := range append(s.Directories(), s.Protected...) {
		if Overlaps(cfg.Paths.ArtifactsDir, dir) {
			return "", fmt.Errorf("artifact cache must be separate from service and Kafka paths")
		}
	}
	if err = os.MkdirAll(cfg.Paths.ArtifactsDir, 0750); err != nil {
		return "", err
	}
	// Private staging directory prevents concurrent tasks from sharing extraction paths.
	staging, err := os.MkdirTemp(cfg.Paths.ArtifactsDir, "data-service-")
	if err != nil {
		return "", err
	}
	defer func() {
		if e := os.RemoveAll(staging); e != nil {
			logs.WriteString("\n[WARN] Staging retained: " + staging)
		}
	}()
	archive := filepath.Join(staging, "artifact.tgz")
	if _, err = download.DownloadArtifact(task.ArtifactURL, archive); err != nil {
		return logs.String(), err
	}
	if err = checksum.VerifySHA256(archive, task.Checksum); err != nil {
		return logs.String(), err
	}
	root, err := InspectArchive(archive, kind)
	if err != nil {
		return logs.String(), fmt.Errorf("artifact validation: %w", err)
	}
	logs.WriteString("[PASS] Artifact checksum, launchers, libraries and internal links\n")
	// Repeat mandatory host checks immediately before installation, with no deferrals.
	precheckLog, err := Precheck(ctx, exec, kind, task)
	logs.WriteString(precheckLog + "\n")
	if err != nil {
		return logs.String(), err
	}
	out, errOut, err := exec.Run(ctx, "bash", "-c", `command -v java | xargs readlink -f`)
	javaHome := path.Dir(path.Dir(strings.TrimSpace(out)))
	if err != nil || SafePath(javaHome) != nil {
		return logs.String(), fmt.Errorf("cannot locate Java home: %s", errOut)
	}
	extracted := filepath.Join(staging, "distribution")
	if err = os.Mkdir(extracted, 0750); err != nil {
		return logs.String(), err
	}
	// Extract without root: validated archives may not write through links or escape staging.
	if _, errOut, err = exec.Run(ctx, "tar", "-xzf", archive, "-C", extracted, "--no-same-owner", "--no-same-permissions"); err != nil {
		return logs.String(), fmt.Errorf("extract artifact: %w: %s", err, errOut)
	}
	if err = CheckPaths(ctx, exec, s); err != nil {
		return logs.String(), err
	}
	if err = run("mkdir", append([]string{"-p", "--"}, s.Directories()...)...); err != nil {
		return logs.String(), err
	}
	// Copy only runtime trees. Config/log/plugin/working directories are never overwritten by the archive.
	source := filepath.Join(extracted, root)
	for _, tree := range []string{"bin", "libs", "share"} {
		if _, statErr := os.Stat(filepath.Join(source, tree)); os.IsNotExist(statErr) {
			continue
		}
		if err = run("cp", "-a", "--", filepath.Join(source, tree), s.Install+"/"); err != nil {
			return logs.String(), err
		}
	}
	files := ConfigFiles(s, javaHome)
	for _, file := range files {
		tmp, createErr := os.CreateTemp(staging, "config-")
		if createErr != nil {
			return logs.String(), createErr
		}
		_, writeErr := tmp.WriteString(file.Content)
		closeErr := tmp.Close()
		if writeErr != nil {
			return logs.String(), writeErr
		}
		if closeErr != nil {
			return logs.String(), closeErr
		}
		// install replaces the destination inode rather than following a final symlink.
		if err = run("install", "-m", "0644", "--", tmp.Name(), file.Path); err != nil {
			return logs.String(), err
		}
	}
	if err = run("chown", append([]string{"-R", "--", s.User + ":" + s.Group}, s.Directories()...)...); err != nil {
		return logs.String(), err
	}
	if err = run("systemctl", "daemon-reload"); err != nil {
		return logs.String(), err
	}
	if err = run("systemctl", "enable", kind+".service"); err != nil {
		return logs.String(), err
	}
	if err = run("systemctl", "start", kind+".service"); err != nil {
		return logs.String(), err
	}
	health, err := Verify(ctx, kind, "http://"+net.JoinHostPort("127.0.0.1", strconv.Itoa(s.Port)))
	logs.WriteString(health)
	return logs.String(), err
}

type ConfigFile struct{ Path, Content string }

func ConfigFiles(s Settings, javaHome string) []ConfigFile {
	propertyName := "connect-distributed.properties"
	launcher := "connect-distributed.sh"
	heapVariable := "KAFKA_HEAP_OPTS"
	logVariable := "KAFKA_LOG4J_OPTS"
	properties := fmt.Sprintf(`# Managed by Tantor
bootstrap.servers=%s
group.id=%s
listeners=http://0.0.0.0:%d
rest.advertised.host.name=%s
rest.advertised.port=%d
rest.advertised.listener=http
plugin.path=%s
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=true
value.converter.schemas.enable=true
offset.storage.topic=%s
config.storage.topic=%s
status.storage.topic=%s
offset.storage.replication.factor=%d
config.storage.replication.factor=%d
status.storage.replication.factor=%d
offset.storage.partitions=25
status.storage.partitions=5
`, s.Bootstrap, s.GroupID, s.Port, s.Host, s.Port, s.Plugin, s.OffsetTopic, s.ConfigTopic, s.StatusTopic, s.Replication, s.Replication, s.Replication)
	if s.Kind == Schema {
		propertyName = "schema-registry.properties"
		launcher = "schema-registry-start"
		heapVariable = "SCHEMA_REGISTRY_HEAP_OPTS"
		logVariable = "SCHEMA_REGISTRY_LOG4J_OPTS"
		endpoints := strings.Split(s.Bootstrap, ",")
		for i := range endpoints {
			endpoints[i] = "PLAINTEXT://" + endpoints[i]
		}
		properties = fmt.Sprintf(`# Managed by Tantor
listeners=http://0.0.0.0:%d
host.name=%s
kafkastore.bootstrap.servers=%s
kafkastore.topic=%s
kafkastore.topic.replication.factor=%d
schema.registry.group.id=%s
schema.compatibility.level=%s
`, s.Port, s.Host, strings.Join(endpoints, ","), s.SchemaTopic, s.Replication, s.GroupID, s.Compatibility)
	}
	// Journald handles retention; avoid sharing Kafka's log config or JVM options.
	logging := `<?xml version="1.0" encoding="UTF-8"?>
<Configuration status="WARN"><Appenders><Console name="Console" target="SYSTEM_OUT"><PatternLayout pattern="%d{ISO8601} %-5p %c - %m%n"/></Console></Appenders><Loggers><Root level="info"><AppenderRef ref="Console"/></Root></Loggers></Configuration>
`
	unit := fmt.Sprintf(`[Unit]
Description=Tantor %s
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=%s
Group=%s
Environment="JAVA_HOME=%s"
Environment="LOG_DIR=%s"
Environment="JMX_PORT="
Environment="%s=-Xms%s -Xmx%s"
Environment="%s=-Dlog4j2.configurationFile=%s/log4j2.xml"
WorkingDirectory=%s
ExecStart=%s/bin/%s %s/%s
Restart=on-failure
RestartSec=5
KillMode=control-group
TimeoutStopSec=180
LimitNOFILE=1024000
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
`, s.Kind, s.User, s.Group, javaHome, s.Log, heapVariable, s.Heap, s.Heap, logVariable, s.Config, s.Working, s.Install, launcher, s.Config, propertyName)
	return []ConfigFile{{s.Config + "/" + propertyName, properties}, {s.Config + "/log4j2.xml", logging}, {"/etc/systemd/system/" + s.Kind + ".service", unit}}
}
