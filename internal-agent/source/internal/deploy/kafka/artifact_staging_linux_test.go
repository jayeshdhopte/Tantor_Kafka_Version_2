//go:build linux

package kafka

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"io.translab/tantor-agent/internal/client"
	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/pkg/api"
)

type stagingExecutor struct{}

func (stagingExecutor) Run(context.Context, string, ...string) (string, string, error) {
	return "", "", nil
}
func (stagingExecutor) RunSudo(_ context.Context, command string, args ...string) (string, string, error) {
	if command == "systemctl" {
		return "inactive", "", nil
	}
	return "", "", nil
}

func TestKafkaFailedAndResumedDownloadsLeaveNoTaskTar(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		root, err := os.MkdirTemp("/var/tmp", "tantor-artifact-staging-test-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(root) })
		var requests atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(status)
			w.Write([]byte("fixture archive bytes"))
		}))
		cfg := &config.Config{}
		cfg.Agent.ServerURL = server.URL
		cfg.Agent.HostID = "test"
		cfg.Auth.Mode = "bearer"
		cfg.Auth.Token = "fixture-token"
		cfg.HTTP.RequestTimeoutSeconds = 5
		cfg.HTTP.ArtifactTimeoutSeconds = 5
		cfg.Paths.ArtifactsDir = filepath.Join(root, "artifacts")
		cfg.Paths.AllowedDeploymentRoots = root
		cfg.Paths.DataDir = filepath.Join(root, "state")
		apiClient, err := client.NewAPIClient(cfg, "test")
		if err != nil {
			t.Fatal(err)
		}
		task := &api.Task{TaskID: "9299c516-9fbd-4559-9a2b-86728e3676a9", ArtifactID: "9299c516-9fbd-4559-9a2b-86728e3676a9", Checksum: strings.Repeat("0", 64), Parameters: map[string]string{"service_role": "broker", "node_id": "1", "kafka_install_base_dir": root, "db_cluster_id": "cluster-a", "version": "4.3.0", "resume_step": "Extract Kafka"}}
		d := &Deployer{cfg: cfg, client: apiClient, exec: stagingExecutor{}}
		if _, err := d.Deploy(context.Background(), task, nil); err == nil {
			t.Fatal("invalid download accepted")
		}
		server.Close()
		if requests.Load() == 0 {
			t.Fatal("resume trusted a missing/stale archive without downloading")
		}
		entries, err := os.ReadDir(cfg.Paths.ArtifactsDir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatal("failed deployment retained task archives", entries)
		}
	}
}
