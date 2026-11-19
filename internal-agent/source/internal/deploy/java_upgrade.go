package deploy

import (
	"context"
	"io.translab/tantor-agent/pkg/api"
	"io.translab/tantor-agent/pkg/javaupgrade"
	"strings"
)

func (e *Engine) upgradeJava(ctx context.Context, task *api.Task) (*api.TaskResult, error) {
	var log strings.Builder
	err := e.runJavaUpgrade(ctx, task, &log)
	if err != nil {
		result := e.fail(task, err.Error())
		result.LogOutput = log.String()
		return result, nil
	}
	return &api.TaskResult{TaskID: task.TaskID, HostID: e.cfg.Agent.HostID, Status: "SUCCESS", LogOutput: log.String()}, nil
}

func (e *Engine) runJavaUpgrade(ctx context.Context, task *api.Task, log *strings.Builder) error {
	return javaupgrade.Run(ctx, e.exec, task, log)
}

// Retain package-local test entry points while both agents share the implementation.
var inspectJavaArchive = javaupgrade.InspectArchive
var javaUpgradeAllowed = javaupgrade.Allowed
