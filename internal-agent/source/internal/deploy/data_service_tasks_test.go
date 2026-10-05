package deploy

import (
	"context"
	"errors"
	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/pkg/api"
	"strings"
	"testing"
)

type absentServiceExecutor struct{}

func (absentServiceExecutor) Run(context.Context, string, ...string) (string, string, error) {
	return "", "", nil
}

func (absentServiceExecutor) RunSudo(_ context.Context, name string, _ ...string) (string, string, error) {
	if name == "test" {
		return "", "", errors.New("unit not installed")
	}
	return "", "", nil
}

func TestClusterCleanupHandlesDataServiceOnlyHost(t *testing.T) {
	cfg := &config.Config{}
	cfg.Agent.HostID = "connect-host"
	engine := NewEngine(cfg, nil, absentServiceExecutor{})
	task := &api.Task{TaskID: "cleanup", Parameters: map[string]string{
		"data_services": `[{"kind":"kafka_connect","bootstrap_servers":"broker:9092","rest_port":8083}]`,
		"cleanup_kafka": "false",
	}}
	result, err := engine.deleteCluster(context.Background(), task)
	if err != nil || result.Status != "SUCCESS" || !strings.Contains(result.LogOutput, "unit is not installed") {
		t.Fatalf("cleanup result=%+v err=%v", result, err)
	}
}

func TestDataServiceCommandsAreRoutedAndFailClosedWithoutConfiguration(t *testing.T) {
	engine := NewEngine(&config.Config{}, nil, nil)
	for _, command := range []string{"PRECHECK_SCHEMA", "PRECHECK_CONNECT", "INSTALL_SCHEMA", "INSTALL_CONNECT", "VERIFY_SCHEMA_REGISTRY", "VERIFY_CONNECT"} {
		result, err := engine.Execute(context.Background(), &api.Task{TaskID: "test", Command: command})
		if err != nil || result == nil || result.Status != "FAILED" || strings.Contains(result.ErrorMsg, "Unknown command") {
			t.Fatalf("%s: result=%+v err=%v", command, result, err)
		}
	}
}
