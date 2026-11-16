package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io.translab/tantor-agent/pkg/addnodereadiness"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"time"
)

const externalReadinessTasksAPI = "/api/v1/ui/external-clusters/discovery/add-node-tasks/"

type externalReadinessTask struct {
	ID         string          `json:"id"`
	Command    string          `json:"command"`
	ClaimToken string          `json:"claimToken"`
	Parameters json.RawMessage `json:"parameters"`
}

// This transport is separate from managed Kafka tasks: an unassigned host must
// never be linked to a cluster just to inspect it. Only INSPECT/PROBE are allowed.
func pollAddNodeReadiness(ctx context.Context, client *APIClient, cfg RuntimeConfig) error {
	if err := validateAddNodeInspectionMode(cfg, os.Geteuid(), runtime.GOOS); err != nil {
		return err
	}
	if !cfg.EnableAddNodeInspection {
		return nil
	}
	endpoint := client.endpoint(externalReadinessTasksAPI + url.PathEscape(cfg.HostID))
	var task externalReadinessTask
	if _, err := client.DoJSON(ctx, http.MethodPost, endpoint+"/claim", nil, map[string]string{}, &task); err != nil {
		return err
	}
	if task.ID == "" {
		return nil
	}
	taskCtx, cancel := context.WithTimeout(ctx, 65*time.Second)
	defer cancel()
	report, err := addnodereadiness.Run(taskCtx, task.Command, string(task.Parameters), cfg.HostID)
	result := map[string]any{"claimToken": task.ClaimToken, "status": "SUCCESS", "report": report}
	if err != nil {
		result["status"] = "FAILED"
		result["report"] = nil
		result["message"] = "Read-only inspection failed; verify the candidate profile and agent permissions."
	}
	// Retry only result delivery, never repeat a completed inspection on a lost response.
	for attempt := 0; attempt < 3; attempt++ {
		if _, err := client.DoJSON(ctx, http.MethodPost, endpoint+"/"+url.PathEscape(task.ID)+"/result", nil, result, nil); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("readiness result delivery failed; preview will expire")
}

func runAddNodeReadinessTasks(ctx context.Context, client *APIClient, cfg RuntimeConfig, logger *slog.Logger) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		if err := pollAddNodeReadiness(ctx, client, cfg); err != nil {
			logger.Warn("External readiness task unavailable", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
