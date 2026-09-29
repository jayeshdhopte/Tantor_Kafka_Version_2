package deploy

import (
	"context"
	"io.translab/tantor-agent/internal/deploy/dataservice"
	"io.translab/tantor-agent/pkg/api"
)

func (e *Engine) precheckDataService(ctx context.Context, t *api.Task, kind string) (*api.TaskResult, error) {
	logs, err := dataservice.Precheck(ctx, e.exec, kind, t)
	return e.dataServiceResult(t, logs, err), nil
}

func (e *Engine) verifyDataService(ctx context.Context, t *api.Task, kind, urlKey string) (*api.TaskResult, error) {
	logs, err := dataservice.Verify(ctx, kind, t.Parameters[urlKey])
	return e.dataServiceResult(t, logs, err), nil
}

func (e *Engine) dataServiceResult(t *api.Task, logs string, err error) *api.TaskResult {
	result := &api.TaskResult{TaskID: t.TaskID, HostID: e.cfg.Agent.HostID, Status: "SUCCESS", LogOutput: logs}
	if err != nil {
		result.Status = "FAILED"
		result.ErrorMsg = err.Error()
		result.FailedReason = err.Error()
	}
	return result
}
