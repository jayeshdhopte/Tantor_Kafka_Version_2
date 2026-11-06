//go:build !linux

package expansion

import (
	"context"
	"fmt"
	"io.translab/tantor-agent/internal/executor"
)

func RunCandidate(context.Context, string, string, map[string]string, executor.Executor, func(string, string) (string, error)) (string, error) {
	return "", fmt.Errorf("quorum expansion requires a root Linux internal agent")
}
func RunParticipant(context.Context, string, string, map[string]string, executor.Executor) (string, error) {
	return "", fmt.Errorf("quorum expansion requires a root Linux internal agent")
}
func InspectRuntime(context.Context, string, string, executor.Executor) (string, error) {
	return "", fmt.Errorf("quorum runtime inspection requires a Linux internal agent")
}
