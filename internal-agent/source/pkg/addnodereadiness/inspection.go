// Package addnodereadiness exposes observation-only checks to the external agent.
// Deployment, formatting and service commands are deliberately not exported.
package addnodereadiness

import (
	"context"
	"encoding/json"
	"fmt"
	"io.translab/tantor-agent/internal/deploy/readiness"
	"io.translab/tantor-agent/internal/executor"
)

func Run(ctx context.Context, command, raw, hostID string) (json.RawMessage, error) {
	if len(raw) > 65536 {
		return nil, fmt.Errorf("inspection request exceeds limit")
	}
	switch command {
	case "INSPECT":
		request, err := readiness.Validate(raw, hostID)
		if err != nil {
			return nil, err
		}
		ctx, cancel := context.WithDeadline(ctx, request.ExpiresAt)
		defer cancel()
		return json.Marshal(readiness.Inspect(ctx, request, hostID, executor.New(executor.Options{PrivilegeMode: "direct"})))
	case "PROBE":
		if err := readiness.Probe(ctx, raw, hostID); err != nil {
			return nil, err
		}
		return json.RawMessage(`{"status":"PASS"}`), nil
	default:
		return nil, fmt.Errorf("unsupported read-only inspection command")
	}
}
