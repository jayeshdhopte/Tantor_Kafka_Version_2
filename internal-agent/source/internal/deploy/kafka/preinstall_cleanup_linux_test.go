//go:build linux

package kafka

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/pkg/api"
	"io.translab/tantor-agent/pkg/artifactwork"
)

type preinstallExecutor struct{ loadState string }

func (e preinstallExecutor) Run(context.Context, string, ...string) (string, string, error) {
	return "", "", nil
}
func (e preinstallExecutor) RunSudo(_ context.Context, command string, args ...string) (string, string, error) {
	if command != "systemctl" || len(args) != 4 || args[0] != "show" {
		panic("cleanup attempted a mutation without deployment state")
	}
	return e.loadState, "", nil
}

func TestApprovedArtifactAliasStagesAndCleansAtRealRoot(t *testing.T) {
	root := t.TempDir()
	real := filepath.Join(t.TempDir(), "cache")
	if err := os.Mkdir(real, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "approved-alias")
	if err := os.Symlink(real, alias); err != nil {
		t.Fatal(err)
	}
	task := &api.Task{Parameters: map[string]string{"artifact_load_dir": alias, "db_cluster_id": "a"}}
	resolved, err := kafkaArtifactWorkDir(task, alias, alias)
	if err != nil || resolved != real {
		t.Fatalf("approved alias: %s %v", resolved, err)
	}
	w, err := artifactwork.New(resolved, artifactwork.Owner{ClusterID: "a", TaskID: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	id := "9299c516-9fbd-4559-9a2b-86728e3676a9"
	archive := filepath.Join(real, "kafka_"+id+".tgz")
	if err := os.WriteFile(archive, []byte("owned"), 0600); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(real, "shared.tgz")
	if err := os.WriteFile(keep, []byte("shared"), 0600); err != nil {
		t.Fatal(err)
	}
	task.Parameters["cleanup_artifact_downloads"] = `[{"taskId":"` + id + `","artifactRoot":"` + alias + `"}]`
	cfg := &config.Config{}
	cfg.Paths.ArtifactsDir, cfg.Paths.AllowedDeploymentRoots = alias, alias
	d := &Deployer{cfg: cfg}
	if err := d.cleanupDeploymentArtifacts(task, deploymentState{ArtifactRoots: []string{real}}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(archive); !os.IsNotExist(err) {
		t.Fatal("owned archive retained", err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatal("shared archive removed", err)
	}
	if _, err := os.Lstat(alias); err != nil {
		t.Fatal("administrator alias removed", err)
	}
	// Approving a parent does not approve a child alias escaping that parent.
	if _, err := kafkaArtifactWorkDir(task, root, root); err == nil {
		t.Fatal("unapproved child redirection accepted")
	}
	escape := filepath.Join(real, "escape")
	if err := os.Symlink(t.TempDir(), escape); err != nil {
		t.Fatal(err)
	}
	task.Parameters["artifact_load_dir"] = filepath.Join(alias, "escape")
	if _, err := kafkaArtifactWorkDir(task, alias, alias); err == nil {
		t.Fatal("escape beneath approved alias accepted")
	}
}

func TestMissingStateCleanupOnlyRemovesEmptyPreinstallDirectories(t *testing.T) {
	for _, scenario := range []string{"empty", "populated", "redirected", "active", "unit", "corrupt-state"} {
		t.Run(scenario, func(t *testing.T) {
			root, err := os.MkdirTemp("/var/tmp", "tantor-preinstall-cleanup-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.RemoveAll(root) })
			cfg := &config.Config{}
			cfg.Paths.DataDir = filepath.Join(root, "agent-state")
			cfg.Paths.ArtifactsDir, cfg.Paths.AllowedDeploymentRoots = root, root
			task := &api.Task{Parameters: map[string]string{"db_cluster_id": "a", "kafka_install_base_dir": root, "version": "4.3.0", "kafka_data_dir": filepath.Join(root, "data"), "cleanup_artifact_downloads": "[]"}}
			paths := resolveKafkaInstallPaths(task)
			for _, dir := range []string{paths.VersionedDir, task.Parameters["kafka_data_dir"]} {
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			d := &Deployer{cfg: cfg, exec: preinstallExecutor{"not-found"}}
			switch scenario {
			case "populated":
				os.WriteFile(filepath.Join(task.Parameters["kafka_data_dir"], "meta.properties"), []byte("preserve"), 0600)
			case "redirected":
				os.Remove(task.Parameters["kafka_data_dir"])
				os.Symlink(t.TempDir(), task.Parameters["kafka_data_dir"])
			case "active":
				os.Symlink(paths.VersionedDir, paths.ActiveDir)
			case "unit":
				d.exec = preinstallExecutor{"loaded"}
			case "corrupt-state":
				os.MkdirAll(filepath.Dir(d.statePath(task)), 0700)
				os.WriteFile(d.statePath(task), []byte("invalid"), 0600)
			}
			out, err := d.Clean(context.Background(), task)
			if scenario != "empty" {
				if err == nil {
					t.Fatal("unsafe cleanup succeeded", out)
				}
				if _, err := os.Stat(paths.VersionedDir); err != nil {
					t.Fatal("partially deleted before validating whole plan", err)
				}
				return
			}
			if err != nil || !strings.Contains(out, "before installation") {
				t.Fatal(out, err)
			}
			if _, err := os.Stat(paths.VersionedDir); !os.IsNotExist(err) {
				t.Fatal("empty install directory retained", err)
			}
			if _, err := d.Clean(context.Background(), task); err != nil {
				t.Fatal("retry", err)
			}
		})
	}
}
