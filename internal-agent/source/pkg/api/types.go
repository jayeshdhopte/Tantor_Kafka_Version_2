package api

import (
	"encoding/json"
	"strings"
)

// HostRegistration Request for agent to register itself
type HostRegistration struct {
	AgentName   string   `json:"agent_name"`
	HostID      string   `json:"host_id"`
	Hostname    string   `json:"hostname"`
	IPAddresses []string `json:"ip_addresses"`
	OSDetails   string   `json:"os_details"`
	AgentVer    string   `json:"agent_version"`
	AgentPath   string   `json:"agent_path"`
}

// HostHeartbeat Metrics sent periodically
type HostHeartbeat struct {
	HostID      string  `json:"host_id"`
	CPUUsagePct float64 `json:"cpu_usage_pct"`
	MemTotalMB  int64   `json:"mem_total_mb"`
	MemUsedMB   int64   `json:"mem_used_mb"`
	DiskTotalGB int64   `json:"disk_total_gb"`
	DiskUsedGB  int64   `json:"disk_used_gb"`
	JavaVersion string  `json:"java_version"`
}

// Task represents a deployment or management task from the server
type Task struct {
	ClaimToken  string            `json:"claim_token,omitempty"`
	TaskID      string            `json:"task_id"`
	ClusterID   string            `json:"cluster_id,omitempty"`
	Command     string            `json:"command"` // e.g. INSTALL_KAFKA, START_SERVICE
	Parameters  map[string]string `json:"parameters"`
	ArtifactID  string            `json:"artifact_id,omitempty"`
	ArtifactURL string            `json:"artifact_url,omitempty"`
	Checksum    string            `json:"checksum,omitempty"`
}

// UnmarshalJSON keeps the agent compatible with backend payloads that use
// snake_case, camelCase, or explicit download-url aliases for artifact fields.
// The canonical fields remain ArtifactID, ArtifactURL, and Checksum.
func (t *Task) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	// Decode the normal task fields without letting flexible artifact aliases
	// (for example a numeric artifactId) make the whole task fail to decode.
	clean := make(map[string]json.RawMessage, len(raw))
	for key, value := range raw {
		switch key {
		case "artifact_id", "artifactId", "artifactID", "artifact_uuid", "artifactUuid",
			"artifact_url", "artifactUrl", "artifactURL", "download_url", "downloadUrl",
			"artifact_download_url", "artifactDownloadUrl", "file_url", "fileUrl",
			"checksum", "sha256", "sha256sum", "checksum_sha256", "checksumSha256",
			"parameters":
			continue
		default:
			clean[key] = value
		}
	}
	cleanData, err := json.Marshal(clean)
	if err != nil {
		return err
	}
	type taskAlias Task
	var base taskAlias
	if err := json.Unmarshal(cleanData, &base); err != nil {
		return err
	}
	*t = Task(base)
	if parameters, ok := raw["parameters"]; ok && string(parameters) != "null" {
		var values map[string]json.RawMessage
		if err := json.Unmarshal(parameters, &values); err != nil {
			return err
		}
		t.Parameters = make(map[string]string, len(values))
		for key, value := range values {
			if string(value) == "null" {
				continue
			}
			var text string
			if err := json.Unmarshal(value, &text); err == nil {
				t.Parameters[key] = text
			} else {
				// Preserve numbers, booleans, arrays, and objects as JSON text.
				t.Parameters[key] = string(value)
			}
		}
	}

	if strings.TrimSpace(t.Command) == "" {
		t.Command = firstJSONString(raw, "task", "taskType", "task_type", "action")
	}
	t.ClaimToken = firstJSONString(raw, "claim_token", "claimToken")
	t.ArtifactID = firstJSONScalarString(raw, "artifact_id", "artifactId", "artifactID", "artifact_uuid", "artifactUuid")
	t.ArtifactURL = firstJSONString(raw,
		"artifact_url", "artifactUrl", "artifactURL",
		"download_url", "downloadUrl",
		"artifact_download_url", "artifactDownloadUrl",
		"file_url", "fileUrl",
	)
	t.Checksum = firstJSONString(raw, "checksum", "sha256", "sha256sum", "checksum_sha256", "checksumSha256")
	if t.Parameters == nil {
		t.Parameters = map[string]string{}
	}
	return nil
}

func firstJSONString(raw map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		value, ok := raw[key]
		if !ok {
			continue
		}
		var text string
		if err := json.Unmarshal(value, &text); err == nil && strings.TrimSpace(text) != "" {
			return strings.TrimSpace(text)
		}
	}
	return ""
}

func firstJSONScalarString(raw map[string]json.RawMessage, keys ...string) string {
	if value := firstJSONString(raw, keys...); value != "" {
		return value
	}
	for _, key := range keys {
		value, ok := raw[key]
		if !ok {
			continue
		}
		var number json.Number
		if err := json.Unmarshal(value, &number); err == nil && strings.TrimSpace(number.String()) != "" {
			return strings.TrimSpace(number.String())
		}
	}
	return ""
}

// TaskResult reports the result of a task execution
type TaskResult struct {
	ClaimToken   string `json:"claim_token,omitempty"`
	TaskID       string `json:"task_id"`
	HostID       string `json:"host_id"`
	Status       string `json:"status"` // SUCCESS, FAILED
	LogOutput    string `json:"log_output"`
	ErrorMsg     string `json:"error_msg,omitempty"`
	CurrentStep  string `json:"current_step,omitempty"`
	FailedReason string `json:"failed_reason,omitempty"`
}
