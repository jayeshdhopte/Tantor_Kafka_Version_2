package deploy

import (
	"context"
	"encoding/json"
	"fmt"

	"path/filepath"
	"strconv"
	"strings"

	"io.translab/tantor-agent/internal/deploy/dataservice"
	"io.translab/tantor-agent/pkg/api"
)

type schemaDirectoryCandidate struct {
	Path      string `json:"path"`
	Exists    bool   `json:"exists"`
	Writable  bool   `json:"writable"`
	FreeBytes int64  `json:"free_bytes"`
}

func (e *Engine) schemaDirectoryCandidates(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	paths := []string{"/opt", "/opt_apb", "/apache", "/var/lib"}
	if configured := csvValues(t.Parameters["candidate_paths"]); len(configured) > 0 {
		paths = configured
	}
	candidates := make([]schemaDirectoryCandidate, 0, len(paths))
	for _, path := range paths {
		clean, err := safeSchemaPath(path)
		if err != nil {
			continue
		}
		_, _, existsErr := e.exec.RunSudo(ctx, "test", "-d", clean)
		_, _, writableErr := e.exec.RunSudo(ctx, "test", "-w", clean)
		candidates = append(candidates, schemaDirectoryCandidate{
			Path: clean, Exists: existsErr == nil, Writable: writableErr == nil,
			FreeBytes: schemaFreeBytes(ctx, e, clean),
		})
	}
	payload, _ := json.Marshal(map[string]any{"directories": candidates})
	return &api.TaskResult{TaskID: t.TaskID, HostID: e.cfg.Agent.HostID, Status: "SUCCESS", LogOutput: string(payload)}, nil
}

func (e *Engine) precheckSchema(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	return e.precheckDataService(ctx, t, dataservice.Schema)
}

func (e *Engine) verifySchemaRegistry(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	return e.verifyDataService(ctx, t, dataservice.Schema, "schema_registry_url")
}

func endpointAddress(endpoint string) string {
	value := strings.TrimSpace(endpoint)
	if idx := strings.Index(value, "://"); idx >= 0 {
		value = value[idx+3:]
	}
	return value
}

func safeSchemaPath(value string) (string, error) {
	clean := filepath.Clean(strings.TrimSpace(value))
	if clean == "." || clean == "" || !filepath.IsAbs(clean) || clean == string(filepath.Separator) {
		return clean, fmt.Errorf("path must be an absolute non-root path")
	}
	return clean, nil
}

func nearestExistingParent(ctx context.Context, e *Engine, path string) string {
	current := path
	for current != string(filepath.Separator) {
		if _, _, err := e.exec.RunSudo(ctx, "test", "-d", current); err == nil {
			return current
		}
		current = filepath.Dir(current)
	}
	return string(filepath.Separator)
}

func schemaFreeBytes(ctx context.Context, e *Engine, path string) int64 {
	out, _, err := e.exec.Run(ctx, "df", "-Pk", path)
	if err != nil {
		return 0
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 2 {
		return 0
	}
	fields := strings.Fields(lines[len(lines)-1])
	if len(fields) < 4 {
		return 0
	}
	kb, _ := strconv.ParseInt(fields[3], 10, 64)
	return kb * 1024
}

func csvValues(raw string) []string {
	result := []string{}
	for _, value := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func schemaFirstNonBlank(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}
