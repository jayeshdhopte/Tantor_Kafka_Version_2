package connect

import (
	"context"
	"io.translab/tantor-agent/internal/client"
	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/internal/deploy/dataservice"
	"io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/pkg/api"
)

type Deployer struct {
	cfg    *config.Config
	client *client.APIClient
	exec   executor.Executor
}

func NewDeployer(cfg *config.Config, c *client.APIClient, e executor.Executor) *Deployer {
	return &Deployer{cfg, c, e}
}
func (d *Deployer) Deploy(ctx context.Context, t *api.Task) (string, error) {
	return dataservice.Deploy(ctx, d.cfg, d.client, d.exec, dataservice.Connect, t)
}
