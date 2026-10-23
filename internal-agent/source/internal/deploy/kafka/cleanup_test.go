package kafka

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/pkg/api"
)

type clusterCleanupExecutor struct {
	commands      []string
	failRetention bool
}

func (e *clusterCleanupExecutor) Run(ctx context.Context, name string, args ...string) (string, string, error) {
	return e.RunSudo(ctx, name, args...)
}
func (e *clusterCleanupExecutor) RunSudo(_ context.Context, name string, args ...string) (string, string, error) {
	command := name + " " + strings.Join(args, " ")
	e.commands = append(e.commands, command)
	if name == "bash" && e.failRetention {
		return "", "timer could not stop", errors.New("stop failed")
	}
	return "", "", nil
}

func TestClusterCleanupRemovesRoleTimersExportersAndOverrides(t *testing.T) {
	for _, fail := range []bool{false, true} {
		cfg := &config.Config{}
		cfg.Paths.DataDir = t.TempDir()
		run := &clusterCleanupExecutor{failRetention: fail}
		d := &Deployer{cfg: cfg, exec: run}
		task := &api.Task{Parameters: map[string]string{"cluster_id": "owned-cluster"}}
		state := deploymentState{ActiveDir: "/opt/tantor/kafka", VersionedDir: "/opt/tantor/kafka-4.3.0", DataDir: "/data/kafka", Services: []string{"broker", "broker-exporter", "controller"}, Ports: []string{"9092"}, AppLogDirs: []string{"/var/log/tantor/kafka-broker", "/var/log/tantor/kafka-controller"}}
		if err := d.saveDeploymentState(task, state); err != nil {
			t.Fatal(err)
		}
		_, err := d.Clean(context.Background(), task)
		if (err != nil) != fail {
			t.Fatal(err)
		}
		commands := strings.Join(run.commands, "\n")
		if fail {
			if strings.Contains(commands, "rm -rf /data/kafka") {
				t.Fatal("data removed before timer stopped")
			}
			if _, err := os.Stat(d.statePath(task)); err != nil {
				t.Fatal("retry state lost", err)
			}
			continue
		}
		for _, required := range []string{"tantor-kafka-broker broker.log", "tantor-kafka-controller controller.log", "systemctl stop broker-exporter", "rm -rf -- /etc/systemd/system/broker.service.d", "rm -rf /data/kafka"} {
			if !strings.Contains(commands, required) {
				t.Fatal("missing cleanup", required, commands)
			}
		}
		if strings.Contains(commands, "tantor-agent-log-retention") || strings.Contains(commands, "tantor-discovery-agent-log-retention") {
			t.Fatal("shared agent timer affected")
		}
		if state, err := d.loadDeploymentState(task); err != nil || !state.Cleaned {
			t.Fatal("completed cleanup receipt missing", err)
		}
		count := len(run.commands)
		if _, err := d.Clean(context.Background(), task); err != nil || len(run.commands) != count {
			t.Fatal("cleanup retry repeated destructive actions", err)
		}
	}
}
