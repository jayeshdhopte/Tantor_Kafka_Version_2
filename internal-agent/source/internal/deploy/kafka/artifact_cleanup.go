package kafka

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"io.translab/tantor-agent/pkg/api"
	"io.translab/tantor-agent/pkg/artifactwork"
)

type artifactDownload struct {
	TaskID string `json:"taskId"`
	Root   string `json:"artifactRoot"`
}

var artifactTaskID = regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`)

func (d *Deployer) cleanupDeploymentArtifacts(task *api.Task, state deploymentState) error {
	var legacy []artifactDownload
	if raw := task.Parameters["cleanup_artifact_downloads"]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &legacy); err != nil {
			return fmt.Errorf("invalid artifact cleanup plan: %w", err)
		}
	}
	roots := append([]string{}, state.ArtifactRoots...)
	for _, download := range legacy {
		if !artifactTaskID.MatchString(download.TaskID) {
			return fmt.Errorf("artifact cleanup requires a recorded deployment task UUID")
		}
		root, err := kafkaArtifactWorkDir(&api.Task{Parameters: map[string]string{"artifact_load_dir": download.Root}}, d.cfg.Paths.ArtifactsDir, d.cfg.Paths.AllowedDeploymentRoots)
		if err != nil {
			return err
		}
		roots = append(roots, root)
		if _, err := os.Lstat(root); os.IsNotExist(err) {
			continue
		}
		real, err := filepath.EvalSymlinks(root)
		if err != nil || real != root {
			return fmt.Errorf("refusing redirected legacy artifact root %s", root)
		}
		path := filepath.Join(root, "kafka_"+download.TaskID+".tgz")
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("legacy deployment archive is not a regular file: %s", path)
		}
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	for _, root := range uniqueNonEmpty(roots) {
		if _, err := kafkaArtifactWorkDir(&api.Task{Parameters: map[string]string{"artifact_load_dir": root}}, d.cfg.Paths.ArtifactsDir, d.cfg.Paths.AllowedDeploymentRoots); err != nil {
			return err
		}
		if err := artifactwork.CleanupCluster(root, firstNonEmpty(task.Parameters["db_cluster_id"], task.Parameters["cluster_id"])); err != nil {
			return err
		}
	}
	return nil
}
