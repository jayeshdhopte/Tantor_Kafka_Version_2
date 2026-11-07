//go:build !linux

package kafka

import (
	"context"
	"fmt"
	"io.translab/tantor-agent/pkg/api"
)

func (d *Deployer) cleanAddedNodes(_ context.Context, _ *api.Task) (string, error) {
	return "", fmt.Errorf("added-node cleanup requires the Linux root adapter")
}
