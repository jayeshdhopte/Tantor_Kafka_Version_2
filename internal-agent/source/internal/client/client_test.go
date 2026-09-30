package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/pkg/api"
)

func testConfig(serverURL string) *config.Config {
	cfg := &config.Config{}
	cfg.Agent.ServerURL = serverURL
	cfg.Agent.HostID = "host-123"
	cfg.Auth.Mode = "bearer"
	cfg.Auth.Token = "secret-token"
	cfg.HTTP.RequestTimeoutSeconds = 30
	cfg.HTTP.ArtifactTimeoutSeconds = 30
	cfg.HTTP.DialTimeoutSeconds = 5
	cfg.HTTP.TLSHandshakeSeconds = 5
	cfg.HTTP.IdleConnTimeoutSeconds = 30
	return cfg
}

func TestRegisterHostUsesConfiguredServerAndBearerAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/agents/register" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret-token" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		if got := r.Header.Get("X-Tantor-Agent-ID"); got != "host-123" {
			t.Fatalf("unexpected agent ID header: %q", got)
		}
		var req api.HostRegistration
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := NewAPIClient(testConfig(server.URL), "test-version")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.RegisterHost(&api.HostRegistration{HostID: "host-123"}); err != nil {
		t.Fatal(err)
	}
}

func TestDownloadArtifactSupportsRelativeURLAndAtomicWrite(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/artifacts/kafka.tgz" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte("artifact-bytes"))
	}))
	defer server.Close()

	client, err := NewAPIClient(testConfig(server.URL), "test-version")
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "nested", "kafka.tgz")
	checksum, err := client.DownloadArtifact("/artifacts/kafka.tgz", dest)
	if err != nil {
		t.Fatal(err)
	}
	if checksum != "6521df166eb07efaf36eba5b6bedefd9d6a252e9c80bab1c99653700ec71473c" {
		t.Fatalf("unexpected computed checksum: %q", checksum)
	}
	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "artifact-bytes" {
		t.Fatalf("unexpected artifact contents: %q", data)
	}
}

func TestManagementServerBasePathIsPreserved(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/control-plane/api/v1/agents/register" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := NewAPIClient(testConfig(server.URL+"/control-plane"), "test-version")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.RegisterHost(&api.HostRegistration{HostID: "host-123"}); err != nil {
		t.Fatal(err)
	}
}

func TestDownloadArtifactReferenceFallsBackToManagementServerForInternalURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/artifacts/a-123/download" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte("kafka-archive"))
	}))
	defer server.Close()

	client, err := NewAPIClient(testConfig(server.URL), "test-version")
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "kafka.tgz")
	checksum, err := client.DownloadArtifactReference("http://127.0.0.1:1/api/v1/artifacts/a-123/download", "", dest)
	if err != nil {
		t.Fatal(err)
	}
	if checksum == "" {
		t.Fatal("expected computed checksum")
	}
	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "kafka-archive" {
		t.Fatalf("unexpected artifact contents: %q", data)
	}
}

func TestDownloadArtifactReferenceResolvesArtifactID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/artifacts/42/download" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte("artifact-by-id"))
	}))
	defer server.Close()

	client, err := NewAPIClient(testConfig(server.URL), "test-version")
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "kafka.tgz")
	if _, err := client.DownloadArtifactReference("", "42", dest); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "artifact-by-id" {
		t.Fatalf("unexpected artifact contents: %q", data)
	}
}

func TestDownloadArtifactReferenceFollowsMetadataDownloadURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/artifacts/abc/download":
			http.NotFound(w, r)
		case "/api/v1/artifacts/download/abc", "/api/artifacts/abc/download", "/api/artifacts/download/abc", "/api/v1/artifacts/abc/file", "/api/artifacts/abc/file", "/artifacts/abc/download":
			http.NotFound(w, r)
		case "/api/v1/artifacts/abc":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"downloadUrl": "/files/kafka.tgz"}})
		case "/files/kafka.tgz":
			_, _ = w.Write([]byte("metadata-resolved-artifact"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewAPIClient(testConfig(server.URL), "test-version")
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "kafka.tgz")
	if _, err := client.DownloadArtifactReference("", "abc", dest); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "metadata-resolved-artifact" {
		t.Fatalf("unexpected artifact contents: %q", data)
	}
}

func TestDownloadArtifactByIDUsesCanonicalManagementServerGET(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected method: %s", r.Method)
		}
		if r.URL.Path != "/api/artifacts/123/download" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if got := r.Header.Get("X-Tantor-Agent-ID"); got != "host-123" {
			t.Fatalf("unexpected agent ID header: %q", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret-token" {
			t.Fatalf("unexpected authorization header: %q", got)
		}
		w.Header().Set("Content-Type", "application/gzip")
		_, _ = w.Write([]byte("kafka-from-191"))
	}))
	defer server.Close()

	client, err := NewAPIClient(testConfig(server.URL), "test-version")
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "kafka.tgz")
	checksum, err := client.DownloadArtifactByID("123", dest)
	if err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("expected one GET request, got %d", requests)
	}
	if checksum == "" {
		t.Fatal("expected computed checksum")
	}
	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "kafka-from-191" {
		t.Fatalf("unexpected artifact contents: %q", data)
	}
}

func TestDownloadArtifactByIDFallsBackToVersionedEndpoint(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		switch r.URL.Path {
		case "/api/artifacts/abc/download":
			http.NotFound(w, r)
		case "/api/v1/artifacts/abc/download":
			_, _ = w.Write([]byte("versioned-artifact"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewAPIClient(testConfig(server.URL), "test-version")
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "kafka.tgz")
	if _, err := client.DownloadArtifactByID("abc", dest); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 || paths[0] != "/api/artifacts/abc/download" || paths[1] != "/api/v1/artifacts/abc/download" {
		t.Fatalf("unexpected endpoint order: %#v", paths)
	}
}

func TestDownloadArtifactReferencePrefersTaskProvidedArtifactServiceURL(t *testing.T) {
	artifactHits := 0
	artifactServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		artifactHits++
		if r.URL.Path != "/api/v1/artifacts/art-123/download" {
			t.Fatalf("unexpected artifact path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte("artifact-from-dedicated-service"))
	}))
	defer artifactServer.Close()

	managementHits := 0
	managementServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		managementHits++
		http.Error(w, "fallback should not be used", http.StatusForbidden)
	}))
	defer managementServer.Close()

	client, err := NewAPIClient(testConfig(managementServer.URL), "test-version")
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "artifacts", "kafka.tgz")
	checksum, err := client.DownloadArtifactReference(artifactServer.URL+"/api/v1/artifacts/art-123/download", "", dest)
	if err != nil {
		t.Fatal(err)
	}
	if checksum == "" {
		t.Fatal("expected computed checksum")
	}
	if artifactHits != 1 {
		t.Fatalf("expected one direct artifact-service request, got %d", artifactHits)
	}
	if managementHits != 0 {
		t.Fatalf("management fallback was contacted even though direct artifact download succeeded: %d", managementHits)
	}
	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "artifact-from-dedicated-service" {
		t.Fatalf("unexpected artifact contents: %q", data)
	}
}
