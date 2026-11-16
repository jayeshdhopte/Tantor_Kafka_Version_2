//go:build !linux

package expansion

import (
	"context"
	"fmt"
	"io.translab/tantor-agent/internal/executor"
)

func ValidateExternalController(context.Context, string, map[string]string, executor.Executor) (string, error) {
	return "", fmt.Errorf("external controller inspection requires Linux root")
}

func VerifyExternalControllerMounts(context.Context, Plan, executor.Executor) error {
	return fmt.Errorf("controller mount validation requires Linux")
}
