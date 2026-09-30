package ksqldb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"io.translab/tantor-agent/internal/client"
	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/internal/pathpolicy"
	"io.translab/tantor-agent/pkg/api"
	"io.translab/tantor-agent/pkg/checksum"
)

type Deployer struct {
	cfg    *config.Config
	client *client.APIClient
	exec   executor.Executor
}

func NewDeployer(cfg *config.Config, client *client.APIClient, exec executor.Executor) *Deployer {
	return &Deployer{
		cfg:    cfg,
		client: client,
		exec:   exec,
	}
}

func (d *Deployer) Deploy(ctx context.Context, t *api.Task) (string, error) {
	var logs strings.Builder
	log := func(msg string, args ...interface{}) {
		logs.WriteString(fmt.Sprintf(msg, args...) + "\n")
	}

	installDir, err := pathpolicy.Resolve(t.Parameters["install_dir"], d.cfg.Paths.AllowedDeploymentRoots, "install_dir")
	if err != nil {
		return logs.String(), err
	}
	artifactDir, err := pathpolicy.Resolve(t.Parameters["artifact_load_dir"], d.cfg.Paths.AllowedDeploymentRoots, "artifact_load_dir")
	if err != nil {
		return logs.String(), err
	}
	kafkaUser := strings.TrimSpace(t.Parameters["service_user"])
	if kafkaUser == "" {
		return logs.String(), fmt.Errorf("service_user is required")
	}

	log("Starting ksqlDB Deployment...")

	// Directories
	for _, dir := range []string{installDir, artifactDir} {
		if err := d.runRequired(ctx, "mkdir", "-p", dir); err != nil {
			return logs.String(), err
		}
	}

	// Artifact Download
	destPath := filepath.Join(artifactDir, fmt.Sprintf("ksqldb_%s.tgz", t.TaskID))
	log("Downloading artifact to %s", destPath)

	downloadedChecksum, err := d.client.DownloadArtifact(t.ArtifactURL, destPath)
	if err != nil {
		return logs.String(), err
	}

	expectedChecksum := t.Checksum
	if expectedChecksum == "" {
		expectedChecksum = downloadedChecksum
	}
	if err := checksum.VerifySHA256(destPath, expectedChecksum); err != nil {
		return logs.String(), err
	}

	// Extract
	tmpDir := filepath.Join(artifactDir, "extract_ksqldb_"+t.TaskID)
	for _, command := range []struct {
		name string
		args []string
	}{
		{"mkdir", []string{"-p", tmpDir}},
		{"tar", []string{"-xzf", destPath, "-C", tmpDir, "--strip-components=1"}},
		{"cp", []string{"-r", tmpDir + "/.", installDir + "/"}},
		{"rm", []string{"-rf", tmpDir}},
	} {
		if err := d.runRequired(ctx, command.name, command.args...); err != nil {
			return logs.String(), err
		}
	}

	// Configs
	if err := d.generateConfigs(ctx, t, installDir); err != nil {
		return logs.String(), err
	}

	// Permissions
	if err := d.runRequired(ctx, "chown", "-R", kafkaUser+":"+kafkaUser, installDir); err != nil {
		return logs.String(), err
	}

	// Systemd
	if err := d.createSystemdService(ctx, kafkaUser, installDir); err != nil {
		return logs.String(), err
	}

	// Start
	if err := d.runRequired(ctx, "systemctl", "daemon-reload"); err != nil {
		return logs.String(), err
	}
	if err := d.runRequired(ctx, "systemctl", "enable", "--now", "ksqldb-server"); err != nil {
		return logs.String(), err
	}

	log("ksqlDB Deployed and Started successfully")
	return logs.String(), nil
}

func (d *Deployer) generateConfigs(ctx context.Context, t *api.Task, installDir string) error {
	bootstrap := t.Parameters["bootstrap_servers"]
	if bootstrap == "" {
		return fmt.Errorf("bootstrap_servers is required")
	}
	schemaUrl := t.Parameters["schema_registry_url"]
	if schemaUrl == "" {
		return fmt.Errorf("schema_registry_url is required")
	}

	props := struct {
		BootstrapServers  string
		SchemaRegistryUrl string
	}{
		BootstrapServers:  bootstrap,
		SchemaRegistryUrl: schemaUrl,
	}

	return d.writeTemplateToSudoFile(ctx, KsqlServerPropertiesTemplate, props, filepath.Join(installDir, "etc/ksqldb/ksql-server.properties"))
}

func (d *Deployer) createSystemdService(ctx context.Context, user, installDir string) error {
	out, errOut, err := d.exec.Run(ctx, "readlink", "-f", "/usr/bin/java")
	if err != nil {
		return fmt.Errorf("resolve Java executable: %w (%s)", err, errOut)
	}
	javaHome := filepath.Dir(filepath.Dir(strings.TrimSpace(out)))
	if javaHome == "" || javaHome == "." {
		javaHome = "/usr"
	}

	props := struct {
		User       string
		Group      string
		JavaHome   string
		InstallDir string
	}{
		User:       user,
		Group:      user,
		JavaHome:   javaHome,
		InstallDir: installDir,
	}

	return d.writeTemplateToSudoFile(ctx, SystemdTemplate, props, "/etc/systemd/system/ksqldb-server.service")
}

func (d *Deployer) writeTemplateToSudoFile(ctx context.Context, tmplStr string, data interface{}, dest string) error {
	tmpl, err := template.New("tmpl").Parse(tmplStr)
	if err != nil {
		return err
	}
	tmpFile, err := os.CreateTemp("", "ksqldb-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())

	if err := tmpl.Execute(tmpFile, data); err != nil {
		return err
	}
	tmpFile.Close()

	return d.runRequired(ctx, "cp", tmpFile.Name(), dest)
}

func (d *Deployer) runRequired(ctx context.Context, name string, args ...string) error {
	out, errOut, err := d.exec.RunSudo(ctx, name, args...)
	if err != nil {
		return fmt.Errorf("required command %s failed: %w (stdout=%q stderr=%q)", name, err, out, errOut)
	}
	return nil
}
