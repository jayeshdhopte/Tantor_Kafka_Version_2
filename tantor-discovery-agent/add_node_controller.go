package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io.translab/tantor-agent/pkg/addnodecontroller"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"time"
)

const externalControllerAPI = "/api/v1/ui/external-clusters/discovery/controller-tasks/"

func pollExternalControllerTask(ctx context.Context, client *APIClient, cfg RuntimeConfig) error {
	if !cfg.EnableAddNodeInspection {
		return nil
	}
	if err := validateAddNodeInspectionMode(cfg, os.Geteuid(), runtime.GOOS); err != nil {
		return err
	}
	endpoint := client.endpoint(externalControllerAPI + url.PathEscape(cfg.HostID))
	var task externalBrokerTask
	if _, err := client.DoJSON(ctx, http.MethodPost, endpoint+"/claim", nil, map[string]bool{"executionEnabled": cfg.EnableControllerAddition, "separateExecutionEnabled": cfg.EnableSeparateAddition}, &task); err != nil {
		return err
	}
	if task.ID == "" {
		return nil
	}
	proof, err := withBrokerLease(ctx, task.ExpiresAt, func(taskCtx context.Context) (string, error) {
		download := func(id, dest string) (string, error) {
			if !regexp.MustCompile(`^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$`).MatchString(id) {
				return "", fmt.Errorf("approved artifact ID required")
			}
			return (serviceDownloader{client: client, ctx: taskCtx}).DownloadArtifact(client.endpoint("/api/v1/artifacts/"+id+"/download"), dest)
		}
		var plan struct {
			Layout string `json:"layout"`
		}
		if json.Unmarshal([]byte(task.Parameters["expansion_plan"]), &plan) == nil && plan.Layout == "SEPARATE" {
			return addnodecontroller.RunSeparate(taskCtx, task.Command, cfg.HostID, "/var/lib/tantor-discovery-agent/add-node", task.Parameters, cfg.EnableSeparateAddition, download)
		}
		return addnodecontroller.Run(taskCtx, task.Command, cfg.HostID, "/var/lib/tantor-discovery-agent/add-node", task.Parameters, cfg.EnableControllerAddition, download)
	})
	result := map[string]any{"claimToken": task.ClaimToken, "status": "SUCCESS", "report": json.RawMessage(proof)}
	if err != nil {
		result["status"] = "FAILED"
		result["report"] = nil
		message := err.Error()
		if len(message) > 1024 {
			message = message[:1024]
		}
		result["message"] = "Controller " + task.Command + " failed: " + message + ". Preserve receipts and reconcile actual storage/quorum before resuming."
	}
	for i := 0; i < 3; i++ {
		if _, err := client.DoJSON(ctx, http.MethodPost, endpoint+"/"+url.PathEscape(task.ID)+"/result", nil, result, nil); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("controller result delivery unavailable; keep operation ownership")
}

func runExternalControllerTasks(ctx context.Context, client *APIClient, cfg RuntimeConfig, logger *slog.Logger) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		if err := pollExternalControllerTask(ctx, client, cfg); err != nil {
			logger.Warn("External controller task unavailable", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
