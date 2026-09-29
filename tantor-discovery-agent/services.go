package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	ds "io.translab/tantor-agent/pkg/dataservices"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const serviceAPI = "/api/v1/ui/external-clusters/discovery/service-tasks"

type serviceTask struct {
	ID         string            `json:"id"`
	Command    string            `json:"command"`
	Kind       string            `json:"kind"`
	Parameters map[string]string `json:"parameters"`
	Checksum   string            `json:"checksum"`
	ArtifactID string            `json:"artifactId"`
}
type serviceResult struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

// Direct root execution is intentionally limited to the shared data-service
// deployer. No task can request arbitrary commands or Kafka lifecycle actions.
type serviceExecutor struct{}

func (serviceExecutor) Run(ctx context.Context, name string, args ...string) (string, string, error) {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	return string(out), "", err
}
func (e serviceExecutor) RunSudo(ctx context.Context, name string, args ...string) (string, string, error) {
	return e.Run(ctx, name, args...)
}

type serviceDownloader struct {
	client *APIClient
	ctx    context.Context
}

func (d serviceDownloader) DownloadArtifact(target, dest string) (string, error) {
	base, _ := url.Parse(d.client.baseURL)
	u, err := url.Parse(target)
	if err != nil || u.Scheme != base.Scheme || u.Host != base.Host || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("artifact must use the configured backend origin")
	}
	client := *d.client.http
	client.Timeout = 20 * time.Minute
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	req, err := http.NewRequestWithContext(d.ctx, http.MethodGet, target, nil)
	if err != nil {
		return "", err
	}
	d.client.applyAuth(req)
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("artifact download HTTP %d", resp.StatusCode)
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, err = io.Copy(io.MultiWriter(f, hash), resp.Body)
	closeErr := f.Close()
	if err != nil {
		return "", err
	}
	if closeErr != nil {
		return "", closeErr
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

var serviceUUID = regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`)

func prepareServiceTask(t serviceTask, clusters []DiscoveredCluster) (*ds.Task, error) {
	if t.Command != "PRECHECK" && t.Command != "INSTALL" {
		return nil, fmt.Errorf("unsupported service command")
	}
	if t.Kind != "schema-registry" && t.Kind != "kafka-connect" {
		return nil, fmt.Errorf("unsupported service")
	}
	if !serviceUUID.MatchString(t.ArtifactID) {
		return nil, fmt.Errorf("invalid artifact ID")
	}
	p := make(map[string]string)
	for k, v := range t.Parameters {
		p[k] = v
	}
	p["allow_deferred_kafka"] = "false"
	p["service_user"] = "root"
	p["service_group"] = "root"
	var protected []string
	matched := false
	for _, c := range clusters {
		for _, raw := range []string{c.InstallPath, filepath.Dir(c.PropsFile), c.DataDirs, c.LogDirs} {
			for _, dir := range strings.Split(raw, ",") {
				if strings.TrimSpace(dir) != "" {
					protected = append(protected, strings.TrimSpace(dir))
				}
			}
		}
		// Also protect metadata.log.dir and all log.dirs read directly from the
		// discovered config. Server-supplied paths cannot weaken local protection.
		content, err := os.ReadFile(c.PropsFile)
		if err != nil {
			return nil, fmt.Errorf("cannot inspect discovered Kafka config: %w", err)
		}
		for _, line := range strings.Split(string(content), "\n") {
			k, v, ok := strings.Cut(strings.TrimSpace(line), "=")
			if ok && (strings.TrimSpace(k) == "metadata.log.dir" || strings.TrimSpace(k) == "log.dirs" || strings.TrimSpace(k) == "log.dir") {
				for _, dir := range strings.Split(v, ",") {
					protected = append(protected, strings.TrimSpace(dir))
				}
			}
		}
		same := p["kafka_cluster_id"] != "" && c.KafkaClusterID == p["kafka_cluster_id"]
		if p["kafka_cluster_id"] == "" {
			for _, a := range strings.Split(c.BootstrapServers, ",") {
				for _, b := range strings.Split(p["bootstrap_servers"], ",") {
					if strings.TrimSpace(a) != "" && strings.TrimSpace(a) == strings.TrimSpace(b) {
						same = true
					}
				}
			}
		}
		if same && c.IsRunning {
			matched = true
			p["kafka_install_dir"] = c.InstallPath
		}
	}
	if !matched {
		return nil, fmt.Errorf("target cluster is not currently discovered and running on this agent")
	}
	data, _ := json.Marshal(protected)
	p["protected_paths"] = string(data)
	return &ds.Task{TaskID: t.ID, Parameters: p, Checksum: t.Checksum}, nil
}

func executeServiceTask(ctx context.Context, client *APIClient, cfg RuntimeConfig, store *ClusterStore, t serviceTask) serviceResult {
	task, err := prepareServiceTask(t, store.Get())
	if err != nil {
		return serviceResult{"FAILED", err.Error()}
	}
	task.ArtifactURL = client.baseURL + "/api/v1/artifacts/" + t.ArtifactID + "/download"
	output, err := ds.Execute(ctx, t.Command, t.Kind, filepath.Join(cfg.ServiceStateDir, "artifacts"), task, serviceExecutor{}, serviceDownloader{client, ctx})
	if err != nil {
		return serviceResult{"FAILED", output + "\n" + err.Error()}
	}
	return serviceResult{"SUCCESS", output}
}

func runServicePoller(ctx context.Context, client *APIClient, cfg RuntimeConfig, store *ClusterStore, logger *slog.Logger) {
	if !filepath.IsAbs(cfg.ServiceStateDir) || filepath.Clean(cfg.ServiceStateDir) == "/" {
		logger.Error("invalid service-state-dir")
		return
	}
	if err := os.MkdirAll(cfg.ServiceStateDir, 0700); err != nil {
		logger.Error("cannot create service task journal", "error", err)
		return
	}
	query := url.Values{"agentId": {cfg.HostID}}
	ticker := time.NewTicker(cfg.TaskPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		var t serviceTask
		_, err := client.DoJSON(ctx, http.MethodPost, client.baseURL+serviceAPI+"/claim", query, nil, &t)
		if err != nil {
			logger.Warn("service task poll failed", "error", err)
			continue
		}
		if t.ID == "" {
			continue
		}
		if !serviceUUID.MatchString(t.ID) {
			logger.Error("invalid service task ID")
			continue
		}
		file := filepath.Join(cfg.ServiceStateDir, t.ID+".json")
		result := serviceResult{}
		if b, err := os.ReadFile(file); err == nil {
			if json.Unmarshal(b, &result) != nil {
				result = serviceResult{"FAILED", "Invalid local task journal; inspect the host before retrying"}
			}
		} else {
			// Persist uncertainty before mutations. A crash never silently replays an install.
			result = serviceResult{"FAILED", "Agent interrupted during task; inspect installed service before retrying"}
			b, _ := json.Marshal(result)
			if err := os.WriteFile(file, b, 0600); err != nil {
				logger.Error("cannot journal task", "error", err)
				continue
			}
			taskCtx, cancel := context.WithTimeout(ctx, 25*time.Minute)
			result = executeServiceTask(taskCtx, client, cfg, store, t)
			cancel()
			b, _ = json.Marshal(result)
			if err := os.WriteFile(file+".tmp", b, 0600); err != nil {
				logger.Error("cannot save task result", "error", err)
				continue
			}
			if err := os.Rename(file+".tmp", file); err != nil {
				logger.Error("cannot commit task result", "error", err)
				continue
			}
		}
		_, err = client.DoJSON(ctx, http.MethodPost, client.baseURL+serviceAPI+"/"+t.ID+"/complete", query, result, nil)
		if err != nil {
			logger.Warn("service result pending delivery", "error", err)
		}
	}
}
