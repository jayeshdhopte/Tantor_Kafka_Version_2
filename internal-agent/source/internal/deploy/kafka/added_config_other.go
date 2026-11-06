//go:build !linux

package kafka

import (
	"fmt"
	"io.translab/tantor-agent/pkg/api"
)

func validateAddedConfig(_ *api.Task, _, _ string) (string, error) {
	return "", fmt.Errorf("added-node configuration requires a Linux root agent")
}
