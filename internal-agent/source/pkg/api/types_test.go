package api

import (
	"encoding/json"
	"testing"
)

func TestTaskAcceptsCamelCaseArtifactFields(t *testing.T) {
	var task Task
	payload := []byte(`{
		"task_id":"task-1",
		"command":"INSTALL_KAFKA",
		"artifactId":"artifact-123",
		"artifactDownloadUrl":"/api/artifacts/artifact-123/download",
		"checksumSha256":"abc",
		"parameters":{}
	}`)
	if err := json.Unmarshal(payload, &task); err != nil {
		t.Fatal(err)
	}
	if task.ArtifactID != "artifact-123" {
		t.Fatalf("unexpected artifact ID: %q", task.ArtifactID)
	}
	if task.ArtifactURL != "/api/artifacts/artifact-123/download" {
		t.Fatalf("unexpected artifact URL: %q", task.ArtifactURL)
	}
	if task.Checksum != "abc" {
		t.Fatalf("unexpected checksum: %q", task.Checksum)
	}
}

func TestTaskAcceptsReferenceFlowTaskAliasAndNumericArtifactID(t *testing.T) {
	var task Task
	payload := []byte(`{
		"task_id":"task-2",
		"task":"INSTALL_KAFKA",
		"artifactId":123,
		"parameters":{}
	}`)
	if err := json.Unmarshal(payload, &task); err != nil {
		t.Fatal(err)
	}
	if task.Command != "INSTALL_KAFKA" {
		t.Fatalf("unexpected command: %q", task.Command)
	}
	if task.ArtifactID != "123" {
		t.Fatalf("unexpected artifact ID: %q", task.ArtifactID)
	}
}

func TestTaskAcceptsNumericSnakeCaseArtifactID(t *testing.T) {
	var task Task
	payload := []byte(`{
		"task_id":"task-3",
		"command":"INSTALL_KAFKA",
		"artifact_id":456,
		"parameters":{}
	}`)
	if err := json.Unmarshal(payload, &task); err != nil {
		t.Fatal(err)
	}
	if task.ArtifactID != "456" {
		t.Fatalf("unexpected artifact ID: %q", task.ArtifactID)
	}
}

func TestTaskClaimTokenAliases(t *testing.T) {
	for _, key := range []string{"claim_token", "claimToken"} {
		var task Task
		if err := json.Unmarshal([]byte(`{"task_id":"t","`+key+`":"claim-123"}`), &task); err != nil {
			t.Fatal(err)
		}
		if task.ClaimToken != "claim-123" {
			t.Fatalf("%s lost claim token", key)
		}
	}
}

func TestTaskAcceptsMixedParameterTypes(t *testing.T) {
	var task Task
	payload := []byte(`{"task_id":"task-4","command":"INSTALL_SCHEMA","parameters":{"rest_port":8084,"enabled":true,"host_id":"node174.translab.io","optional":null,"config":{"a":1}}}`)
	if err := json.Unmarshal(payload, &task); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"rest_port": "8084", "enabled": "true", "host_id": "node174.translab.io", "config": `{"a":1}`} {
		if got := task.Parameters[key]; got != want {
			t.Errorf("parameter %s: got %q, want %q", key, got, want)
		}
	}
	if _, ok := task.Parameters["optional"]; ok {
		t.Fatal("null parameter should be omitted")
	}
}
