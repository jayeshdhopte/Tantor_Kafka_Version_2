package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io.translab/tantor-agent/pkg/addnodebroker"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"time"
)

const externalBrokerTasksAPI = "/api/v1/ui/external-clusters/discovery/add-broker-tasks/"

type externalBrokerTask struct {
	ID         string            `json:"id"`
	Command    string            `json:"command"`
	ClaimToken string            `json:"claimToken"`
	Parameters map[string]string `json:"parameters"`
}

func pollExternalBrokerTask(ctx context.Context, client *APIClient, cfg RuntimeConfig) error {
	if !cfg.EnableAddNodeExecution {
		return nil
	}
	if err := validateAddNodeInspectionMode(cfg, os.Geteuid(), runtime.GOOS); err != nil {
		return err
	}
	endpoint := client.endpoint(externalBrokerTasksAPI + url.PathEscape(cfg.HostID))
	var task externalBrokerTask
	if _, err := client.DoJSON(ctx, http.MethodPost, endpoint+"/claim", nil, map[string]string{}, &task); err != nil {
		return err
	}
	if task.ID == "" {
		return nil
	}
	taskCtx, cancel := context.WithTimeout(ctx, 180*time.Second)
	defer cancel()
	download := func(id, dest string) (string, error) {
		if !regexp.MustCompile(`^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$`).MatchString(id) {
			return "", fmt.Errorf("approved artifact ID required")
		}
		return (serviceDownloader{client: client, ctx: taskCtx}).DownloadArtifact(client.endpoint("/api/v1/artifacts/"+id+"/download"), dest)
	}
	proof, err := addnodebroker.Run(taskCtx, task.Command, cfg.HostID, "/var/lib/tantor-discovery-agent/add-node", task.Parameters, download)
	result := map[string]any{"claimToken": task.ClaimToken, "status": "SUCCESS", "report": json.RawMessage(proof)}
	if err != nil {
		result["status"] = "FAILED"
		result["report"] = nil
		result["message"] = "Broker action failed; preserve operation receipts and reservations for reconciliation."
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
	return fmt.Errorf("broker result delivery unavailable; operation requires reconciliation")
}
func runExternalBrokerTasks(ctx context.Context, client *APIClient, cfg RuntimeConfig, logger *slog.Logger) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		if err := pollExternalBrokerTask(ctx, client, cfg); err != nil {
			logger.Warn("External broker task unavailable", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
