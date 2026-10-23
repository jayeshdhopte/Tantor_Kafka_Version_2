package kafka

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/pkg/api"
	"io.translab/tantor-agent/pkg/artifactwork"
)

func TestDeletionRemovesRecordedArchivesAndPreservesSharedFiles(t *testing.T) {
	root := t.TempDir()
	cfg := &config.Config{}
	cfg.Paths.ArtifactsDir = root
	cfg.Paths.AllowedDeploymentRoots = root
	d := &Deployer{cfg: cfg}
	id := "9299c516-9fbd-4559-9a2b-86728e3676a9"
	archive := filepath.Join(root, "kafka_"+id+".tgz")
	os.WriteFile(archive, []byte("owned"), 0600)
	keep := filepath.Join(root, "kafka_00000000-0000-0000-0000-000000000000.tgz")
	os.WriteFile(keep, []byte("other cluster"), 0600)
	parcel := filepath.Join(root, "parcel_KAFKA_4.3.0.tgz")
	os.WriteFile(parcel, []byte("shared parcel"), 0600)
	own, _ := artifactwork.New(root, artifactwork.Owner{ClusterID: "cluster-a", TaskID: id})
	other, _ := artifactwork.New(root, artifactwork.Owner{ClusterID: "cluster-b", TaskID: id})
	plan, _ := json.Marshal([]artifactDownload{{TaskID: id, Root: root}})
	task := &api.Task{Parameters: map[string]string{"cluster_id": "cluster-a", "cleanup_artifact_downloads": string(plan)}}
	if err := d.cleanupDeploymentArtifacts(task, deploymentState{ArtifactRoots: []string{root}}); err == nil {
		t.Fatal("active deployment staging was removed")
	}
	if _, err := os.Stat(own.Dir()); err != nil {
		t.Fatal("active workspace changed", err)
	}
	if err := own.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.cleanupDeploymentArtifacts(task, deploymentState{ArtifactRoots: []string{root}}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{archive, own.Dir()} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("owned staging retained", path, err)
		}
	}
	for _, path := range []string{keep, parcel, other.Dir()} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal("unrelated artifact removed", path, err)
		}
	}
	if err := d.cleanupDeploymentArtifacts(task, deploymentState{}); err != nil {
		t.Fatal("retry", err)
	}
}
func TestArtifactCleanupRejectsTraversalAndUnapprovedRoots(t *testing.T) {
	root := t.TempDir()
	cfg := &config.Config{}
	cfg.Paths.ArtifactsDir = root
	cfg.Paths.AllowedDeploymentRoots = root
	d := &Deployer{cfg: cfg}
	for _, plan := range []artifactDownload{{TaskID: "../../etc/passwd", Root: root}, {TaskID: "9299c516-9fbd-4559-9a2b-86728e3676a9", Root: filepath.Dir(root)}} {
		data, _ := json.Marshal([]artifactDownload{plan})
		if d.cleanupDeploymentArtifacts(&api.Task{Parameters: map[string]string{"cluster_id": "a", "cleanup_artifact_downloads": string(data)}}, deploymentState{}) == nil {
			t.Fatal("unsafe cleanup accepted", plan)
		}
	}
}
