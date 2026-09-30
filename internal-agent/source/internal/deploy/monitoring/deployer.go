package monitoring

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
	tantorUser := strings.TrimSpace(t.Parameters["service_user"])
	if tantorUser == "" {
		return logs.String(), fmt.Errorf("service_user is required")
	}

	// Runtime identities are administrator-controlled prerequisites; the UI
	// cannot create arbitrary privileged accounts.
	if _, errOut, checkErr := d.exec.Run(ctx, "id", tantorUser); checkErr != nil {
		return logs.String(), fmt.Errorf("service_user %q does not exist: %w (%s)", tantorUser, checkErr, errOut)
	}

	log("Starting Monitoring Deployment...")

	for _, dir := range []string{installDir, artifactDir} {
		if err := d.runRequired(ctx, "mkdir", "-p", dir); err != nil {
			return logs.String(), err
		}
	}

	// Deploy Prometheus
	if err := d.deployComponent(ctx, t, "prometheus", filepath.Join(installDir, "prometheus"), artifactDir, tantorUser); err != nil {
		return logs.String(), err
	}
	log("Prometheus deployed")

	// Deploy Grafana
	if err := d.deployComponent(ctx, t, "grafana", filepath.Join(installDir, "grafana"), artifactDir, tantorUser); err != nil {
		return logs.String(), err
	}
	log("Grafana deployed")

	// Systemd and Start
	if err := d.runRequired(ctx, "systemctl", "daemon-reload"); err != nil {
		return logs.String(), err
	}
	if err := d.runRequired(ctx, "systemctl", "enable", "--now", "prometheus", "grafana"); err != nil {
		return logs.String(), err
	}

	log("Monitoring Stack Deployed and Started successfully")
	return logs.String(), nil
}

func (d *Deployer) deployComponent(ctx context.Context, t *api.Task, component, installDir, artifactDir, user string) error {
	destPath := filepath.Join(artifactDir, fmt.Sprintf("%s_%s.tgz", component, t.TaskID))

	// We assume artifact URLs are passed as prometheus_url, grafana_url in parameters
	urlParam := component + "_url"
	artifactUrl := t.Parameters[urlParam]
	if artifactUrl == "" {
		return fmt.Errorf("missing artifact url for %s", component)
	}

	downloadedChecksum, err := d.client.DownloadArtifact(artifactUrl, destPath)
	if err != nil {
		return err
	}
	expectedChecksum := strings.TrimSpace(t.Parameters[component+"_checksum"])
	if expectedChecksum == "" {
		return fmt.Errorf("missing checksum for %s", component)
	}
	if downloadedChecksum != "" && !strings.EqualFold(downloadedChecksum, expectedChecksum) {
		return fmt.Errorf("%s download checksum does not match task checksum", component)
	}
	if err := checksum.VerifySHA256(destPath, expectedChecksum); err != nil {
		return err
	}

	tmpDir := filepath.Join(artifactDir, "extract_"+component+"_"+t.TaskID)
	for _, command := range []struct {
		name string
		args []string
	}{
		{"mkdir", []string{"-p", installDir}},
		{"mkdir", []string{"-p", tmpDir}},
		{"tar", []string{"-xzf", destPath, "-C", tmpDir, "--strip-components=1"}},
		{"cp", []string{"-r", tmpDir + "/.", installDir + "/"}},
		{"rm", []string{"-rf", tmpDir}},
	} {
		if err := d.runRequired(ctx, command.name, command.args...); err != nil {
			return err
		}
	}

	// Generate configs
	if component == "prometheus" {
		if err := d.writeTemplateToSudoFile(ctx, PrometheusConfigTemplate, nil, filepath.Join(installDir, "prometheus.yml")); err != nil {
			return err
		}

		props := struct {
			User       string
			Group      string
			InstallDir string
			DataDir    string
		}{
			User:       user,
			Group:      user,
			InstallDir: installDir,
			DataDir:    filepath.Join(installDir, "data"),
		}
		if err := d.writeTemplateToSudoFile(ctx, PrometheusSystemdTemplate, props, "/etc/systemd/system/prometheus.service"); err != nil {
			return err
		}
		if err := d.runRequired(ctx, "mkdir", "-p", props.DataDir); err != nil {
			return err
		}
	} else if component == "grafana" {
		props := struct {
			User       string
			Group      string
			InstallDir string
		}{
			User:       user,
			Group:      user,
			InstallDir: installDir,
		}
		if err := d.writeTemplateToSudoFile(ctx, GrafanaSystemdTemplate, props, "/etc/systemd/system/grafana.service"); err != nil {
			return err
		}
	}

	return d.runRequired(ctx, "chown", "-R", user+":"+user, installDir)
}

func (d *Deployer) writeTemplateToSudoFile(ctx context.Context, tmplStr string, data interface{}, dest string) error {
	tmpl, err := template.New("tmpl").Parse(tmplStr)
	if err != nil {
		return err
	}
	tmpFile, err := os.CreateTemp("", "mon-*")
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
