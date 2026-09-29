// Package dataservices exposes the isolated service deployer to the external
// discovery agent. It deliberately exposes no Kafka deployment operations.
package dataservices

import (
	"context"
	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/internal/deploy/dataservice"
	"io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/pkg/api"
)

type Executor = executor.Executor
type Downloader = dataservice.Downloader
type Task = api.Task

func Execute(ctx context.Context, command, kind, cache string, task *Task, exec Executor, download Downloader) (string, error) {
	if command == "PRECHECK" {
		return dataservice.Precheck(ctx, exec, kind, task)
	}
	cfg := &config.Config{}
	cfg.Paths.ArtifactsDir = cache
	return dataservice.Deploy(ctx, cfg, download, exec, kind, task)
}
