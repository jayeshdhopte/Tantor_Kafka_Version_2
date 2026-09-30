package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/pkg/api"
)

const maxErrorBody = 64 * 1024

// APIClient handles communication with the Tantor management server.
type APIClient struct {
	cfg             *config.Config
	baseURL         *url.URL
	httpClient      *http.Client
	artifactTimeout time.Duration
	authMode        string
	authToken       string
	authUsername    string
	authPassword    string
	userAgent       string
}

func NewAPIClient(cfg *config.Config, version ...string) (*APIClient, error) {
	baseURL, err := url.Parse(cfg.Agent.ServerURL)
	if err != nil {
		return nil, fmt.Errorf("parse management server URL: %w", err)
	}

	transport, err := newTransport(cfg)
	if err != nil {
		return nil, err
	}
	token, username, password, err := loadCredentials(cfg)
	if err != nil {
		return nil, err
	}

	agentVersion := "dev"
	if len(version) > 0 && strings.TrimSpace(version[0]) != "" {
		agentVersion = strings.TrimSpace(version[0])
	}

	return &APIClient{
		cfg:             cfg,
		baseURL:         baseURL,
		httpClient:      &http.Client{Transport: transport, Timeout: time.Duration(cfg.HTTP.RequestTimeoutSeconds) * time.Second},
		artifactTimeout: time.Duration(cfg.HTTP.ArtifactTimeoutSeconds) * time.Second,
		authMode:        cfg.Auth.Mode,
		authToken:       token,
		authUsername:    username,
		authPassword:    password,
		userAgent:       "tantor-agent/" + agentVersion,
	}, nil
}

func newTransport(cfg *config.Config) (*http.Transport, error) {
	dialer := &net.Dialer{
		Timeout:   time.Duration(cfg.HTTP.DialTimeoutSeconds) * time.Second,
		KeepAlive: 30 * time.Second,
	}
	transport := &http.Transport{
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       time.Duration(cfg.HTTP.IdleConnTimeoutSeconds) * time.Second,
		TLSHandshakeTimeout:   time.Duration(cfg.HTTP.TLSHandshakeSeconds) * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: time.Duration(cfg.HTTP.RequestTimeoutSeconds) * time.Second,
	}
	if cfg.HTTP.UseEnvironmentProxy {
		transport.Proxy = http.ProxyFromEnvironment
	}

	serverURL, parseErr := url.Parse(cfg.Agent.ServerURL)
	if parseErr != nil {
		return nil, fmt.Errorf("parse management server URL: %w", parseErr)
	}
	if strings.EqualFold(serverURL.Scheme, "https") {
		tlsConfig := &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: cfg.Agent.InsecureSkipVerify, // explicit opt-in only
		}

		if cfg.Agent.CACert != "" {
			pool, err := x509.SystemCertPool()
			if err != nil || pool == nil {
				pool = x509.NewCertPool()
			}
			pem, err := os.ReadFile(cfg.Agent.CACert)
			if err != nil {
				return nil, fmt.Errorf("read CA certificate %q: %w", cfg.Agent.CACert, err)
			}
			if ok := pool.AppendCertsFromPEM(pem); !ok {
				return nil, fmt.Errorf("CA certificate %q did not contain a valid PEM certificate", cfg.Agent.CACert)
			}
			tlsConfig.RootCAs = pool
		}

		if cfg.Agent.CertFile != "" {
			cert, err := tls.LoadX509KeyPair(cfg.Agent.CertFile, cfg.Agent.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("load client TLS certificate/key: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
		transport.TLSClientConfig = tlsConfig
	}
	return transport, nil
}

func (c *APIClient) RegisterHost(req *api.HostRegistration) error {
	return c.post(context.Background(), "/api/v1/agents/register", req, nil)
}

func (c *APIClient) SendHeartbeat(hb *api.HostHeartbeat) error {
	return c.post(context.Background(), "/api/v1/agents/heartbeat", hb, nil)
}

func (c *APIClient) PollTasks() ([]api.Task, error) {
	var tasks []api.Task
	endpoint, err := c.endpoint("/api/v1/agents/" + url.PathEscape(c.cfg.Agent.HostID) + "/tasks")
	if err != nil {
		return nil, err
	}
	req, err := c.newRequest(context.Background(), http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("poll tasks: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return tasks, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, responseError("poll tasks", resp)
	}
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return nil, fmt.Errorf("decode task response: %w", err)
	}
	return tasks, nil
}

func (c *APIClient) ReportTaskResult(result *api.TaskResult) error {
	return c.post(context.Background(), "/api/v1/agents/tasks/result", result, nil)
}

// DownloadArtifact preserves the original API while using the production
// artifact resolver. Relative paths are resolved against the management
// server, and failed backend-internal URLs are retried through the configured
// management-server origin.
func (c *APIClient) DownloadArtifact(rawURL, destPath string) (string, error) {
	return c.DownloadArtifactReference(rawURL, "", destPath)
}

// DownloadArtifactByID implements the canonical air-gapped artifact flow.
// The artifact must already exist on the configured Tantor management server.
// The agent performs HTTP GET requests only against that configured server and
// streams the response to destPath; it never requires SSH, SCP, or public
// internet access. The unversioned endpoint is tried first because it matches
// the UI/backend artifact contract, followed by the versioned endpoint for
// backward compatibility.
func (c *APIClient) DownloadArtifactByID(artifactID, destPath string) (string, error) {
	artifactID = strings.TrimSpace(artifactID)
	if artifactID == "" {
		return "", fmt.Errorf("artifact_id is required for management-server download")
	}
	escaped := url.PathEscape(artifactID)
	paths := []string{
		"/api/artifacts/" + escaped + "/download",
		"/api/v1/artifacts/" + escaped + "/download",
	}
	var failures []string
	for _, path := range paths {
		endpoint, err := c.endpoint(path)
		if err != nil {
			return "", err
		}
		checksumValue, discoveredURL, downloadErr := c.downloadArtifactCandidate(endpoint, destPath)
		if downloadErr == nil && discoveredURL == "" {
			return checksumValue, nil
		}
		if discoveredURL != "" {
			resolved, resolveErr := c.resolveURL(discoveredURL)
			if resolveErr == nil && sameOrigin(c.baseURL, resolved) {
				checksumValue, _, downloadErr = c.downloadArtifactCandidate(resolved.String(), destPath)
				if downloadErr == nil {
					return checksumValue, nil
				}
			}
		}
		if downloadErr != nil {
			failures = append(failures, fmt.Sprintf("GET %s: %v", safeURL(endpoint), downloadErr))
		}
	}
	return "", fmt.Errorf("artifact %q could not be downloaded from management server %s: %s", artifactID, c.baseURL.String(), strings.Join(failures, " | "))
}

// DownloadArtifactReference downloads an artifact using the best information
// present in a task. Backends in different deployments may provide a direct
// URL, a relative URL, an internal-only URL, an artifact ID, or a metadata URL
// that returns the real download URL. The agent tries only controlled
// management-server fallbacks and never reaches the public internet on its own.
func (c *APIClient) DownloadArtifactReference(rawURL, artifactID, destPath string) (string, error) {
	candidates, err := c.artifactCandidates(rawURL, artifactID)
	if err != nil {
		return "", err
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("artifact reference is empty: task did not contain artifact_url or artifact_id")
	}

	queue := append([]string(nil), candidates...)
	seen := make(map[string]struct{}, len(queue))
	var failures []string

	for len(queue) > 0 {
		candidate := strings.TrimSpace(queue[0])
		queue = queue[1:]
		if candidate == "" {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}

		checksumValue, discoveredURL, downloadErr := c.downloadArtifactCandidate(candidate, destPath)
		if downloadErr == nil && discoveredURL == "" {
			return checksumValue, nil
		}
		if discoveredURL != "" {
			more, resolveErr := c.artifactCandidates(discoveredURL, "")
			if resolveErr != nil {
				failures = append(failures, fmt.Sprintf("%s returned an invalid download URL: %v", safeURL(candidate), resolveErr))
				continue
			}
			queue = append(more, queue...)
			continue
		}
		if downloadErr != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", safeURL(candidate), downloadErr))
		}
	}

	if len(failures) > 8 {
		failures = failures[:8]
	}
	return "", fmt.Errorf("artifact download failed after trying management-server-compatible locations: %s", strings.Join(failures, " | "))
}

func (c *APIClient) artifactCandidates(rawURL, artifactID string) ([]string, error) {
	var candidates []string
	add := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		for _, existing := range candidates {
			if existing == value {
				return
			}
		}
		candidates = append(candidates, value)
	}

	rawURL = strings.TrimSpace(rawURL)
	artifactID = strings.TrimSpace(artifactID)
	if rawURL != "" {
		u, err := url.Parse(rawURL)
		if err != nil {
			return nil, fmt.Errorf("parse artifact URL/reference %q: %w", rawURL, err)
		}
		if u.IsAbs() {
			if u.Scheme != "http" && u.Scheme != "https" {
				return nil, fmt.Errorf("unsupported artifact URL scheme %q", u.Scheme)
			}
			add(u.String())
			if !sameOrigin(c.baseURL, u) {
				rewritten := *u
				rewritten.Scheme = c.baseURL.Scheme
				rewritten.Host = c.baseURL.Host
				rewritten.User = nil
				add(rewritten.String())
			}
		} else {
			resolved, resolveErr := c.resolveURL(rawURL)
			if resolveErr != nil {
				return nil, fmt.Errorf("resolve artifact URL %q: %w", rawURL, resolveErr)
			}
			add(resolved.String())
			// Also preserve a configured reverse-proxy base path when the backend
			// sends a root-relative or filename-only reference.
			joined, joinErr := c.endpoint(rawURL)
			if joinErr == nil {
				add(joined)
			}
		}
		if artifactID == "" && looksLikeArtifactID(rawURL) {
			artifactID = rawURL
		}
	}

	if artifactID != "" {
		escaped := url.PathEscape(artifactID)
		paths := []string{
			"/api/v1/artifacts/" + escaped + "/download",
			"/api/v1/artifacts/download/" + escaped,
			"/api/artifacts/" + escaped + "/download",
			"/api/artifacts/download/" + escaped,
			"/api/v1/artifacts/" + escaped + "/file",
			"/api/artifacts/" + escaped + "/file",
			"/artifacts/" + escaped + "/download",
			// Metadata endpoints are intentionally last. If they return JSON with
			// a download URL, downloadArtifactCandidate follows it safely.
			"/api/v1/artifacts/" + escaped,
			"/api/artifacts/" + escaped,
		}
		for _, path := range paths {
			endpoint, endpointErr := c.endpoint(path)
			if endpointErr != nil {
				return nil, endpointErr
			}
			add(endpoint)
		}
	}
	return candidates, nil
}

func (c *APIClient) downloadArtifactCandidate(rawURL, destPath string) (checksumValue, discoveredURL string, err error) {
	artifactURL, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return "", "", fmt.Errorf("parse candidate URL: %w", err)
	}
	if artifactURL.Scheme != "http" && artifactURL.Scheme != "https" {
		return "", "", fmt.Errorf("unsupported artifact URL scheme %q", artifactURL.Scheme)
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.artifactTimeout)
	defer cancel()
	req, err := c.newRequest(ctx, http.MethodGet, artifactURL.String(), nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Accept", "application/octet-stream, application/gzip, application/x-gzip, application/json;q=0.8, */*;q=0.5")
	// Preserve the management credential boundary: never forward backend
	// Authorization headers to a different artifact host.
	if !sameOrigin(c.baseURL, artifactURL) {
		req.Header.Del("Authorization")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("download artifact: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", "", responseError("download artifact", resp)
	}

	contentType := strings.ToLower(resp.Header.Get("Content-Type"))
	if strings.Contains(contentType, "application/json") || strings.Contains(contentType, "+json") {
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
		if readErr != nil {
			return "", "", fmt.Errorf("read artifact metadata response: %w", readErr)
		}
		if discovered := artifactURLFromJSON(body); discovered != "" {
			return "", discovered, nil
		}
		return "", "", fmt.Errorf("artifact endpoint returned JSON metadata but no download URL")
	}
	if strings.Contains(contentType, "text/html") {
		return "", "", fmt.Errorf("artifact endpoint returned HTML instead of an artifact file")
	}

	if err := os.MkdirAll(filepath.Dir(destPath), 0o750); err != nil {
		return "", "", fmt.Errorf("create artifact destination directory: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(destPath), ".tantor-download-*")
	if err != nil {
		return "", "", fmt.Errorf("create temporary artifact file: %w", err)
	}
	tmpPath := tmp.Name()
	keep := false
	defer func() {
		_ = tmp.Close()
		if !keep {
			_ = os.Remove(tmpPath)
		}
	}()

	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(tmp, hasher), resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("write artifact: %w", err)
	}
	if written == 0 {
		return "", "", fmt.Errorf("artifact endpoint returned an empty file")
	}
	computedChecksum := hex.EncodeToString(hasher.Sum(nil))
	if advertised := normalizeSHA256(resp.Header.Get("X-Checksum-SHA256")); advertised != "" && advertised != computedChecksum {
		return "", "", fmt.Errorf("artifact checksum header mismatch: expected %s, downloaded %s", advertised, computedChecksum)
	}
	if err := tmp.Sync(); err != nil {
		return "", "", fmt.Errorf("sync artifact: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", "", fmt.Errorf("close artifact: %w", err)
	}
	if err := os.Chmod(tmpPath, 0o640); err != nil {
		return "", "", fmt.Errorf("set artifact permissions: %w", err)
	}
	if err := os.Rename(tmpPath, destPath); err != nil {
		return "", "", fmt.Errorf("commit artifact download: %w", err)
	}
	keep = true
	return computedChecksum, "", nil
}

func artifactURLFromJSON(body []byte) string {
	var value interface{}
	if err := json.Unmarshal(body, &value); err != nil {
		return ""
	}
	wanted := map[string]struct{}{
		"artifact_url": {}, "artifacturl": {},
		"download_url": {}, "downloadurl": {},
		"artifact_download_url": {}, "artifactdownloadurl": {},
		"file_url": {}, "fileurl": {}, "url": {},
	}
	var walk func(interface{}) string
	walk = func(current interface{}) string {
		switch typed := current.(type) {
		case map[string]interface{}:
			for key, raw := range typed {
				normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(key, "-", "_"), " ", "_"))
				if _, ok := wanted[normalized]; ok {
					if text, ok := raw.(string); ok && strings.TrimSpace(text) != "" {
						return strings.TrimSpace(text)
					}
				}
			}
			for _, raw := range typed {
				if found := walk(raw); found != "" {
					return found
				}
			}
		case []interface{}:
			for _, raw := range typed {
				if found := walk(raw); found != "" {
					return found
				}
			}
		}
		return ""
	}
	return walk(value)
}

func normalizeSHA256(value string) string {
	value = strings.TrimSpace(strings.Trim(value, `"'`))
	value = strings.TrimPrefix(strings.ToLower(value), "sha256:")
	if len(value) != 64 {
		return ""
	}
	if _, err := hex.DecodeString(value); err != nil {
		return ""
	}
	return value
}

func looksLikeArtifactID(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 256 {
		return false
	}
	return !strings.ContainsAny(value, "/\\:?#")
}

func safeURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid-url>"
	}
	u.User = nil
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

func (c *APIClient) post(ctx context.Context, path string, reqBody interface{}, respBody interface{}) error {
	data, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	endpoint, err := c.endpoint(path)
	if err != nil {
		return err
	}
	req, err := c.newRequest(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return responseError("POST "+path, resp)
	}
	if respBody != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(respBody); err != nil {
			return fmt.Errorf("decode response from %s: %w", path, err)
		}
	}
	return nil
}

func (c *APIClient) newRequest(ctx context.Context, method, rawURL string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		return nil, fmt.Errorf("create %s request: %w", method, err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("X-Tantor-Agent-ID", c.cfg.Agent.HostID)
	switch c.authMode {
	case "bearer":
		req.Header.Set("Authorization", "Bearer "+c.authToken)
	case "basic":
		req.SetBasicAuth(c.authUsername, c.authPassword)
	}
	return req, nil
}

func (c *APIClient) endpoint(path string) (string, error) {
	joined, err := url.JoinPath(c.baseURL.String(), strings.TrimLeft(path, "/"))
	if err != nil {
		return "", fmt.Errorf("join management-server URL with API path %q: %w", path, err)
	}
	return joined, nil
}

func (c *APIClient) resolveURL(raw string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, err
	}
	if u.IsAbs() {
		return u, nil
	}
	return c.baseURL.ResolveReference(u), nil
}

func loadCredentials(cfg *config.Config) (token, username, password string, err error) {
	token = strings.TrimSpace(cfg.Auth.Token)
	username = cfg.Auth.Username
	password = cfg.Auth.Password
	if cfg.Auth.TokenFile != "" {
		data, readErr := os.ReadFile(cfg.Auth.TokenFile)
		if readErr != nil {
			return "", "", "", fmt.Errorf("read bearer token file %q: %w", cfg.Auth.TokenFile, readErr)
		}
		token = strings.TrimSpace(string(data))
	}
	if cfg.Auth.PasswordFile != "" {
		data, readErr := os.ReadFile(cfg.Auth.PasswordFile)
		if readErr != nil {
			return "", "", "", fmt.Errorf("read basic-auth password file %q: %w", cfg.Auth.PasswordFile, readErr)
		}
		password = strings.TrimRight(string(data), "\r\n")
	}
	if cfg.Auth.Mode == "bearer" && token == "" {
		return "", "", "", fmt.Errorf("bearer token is empty")
	}
	if cfg.Auth.Mode == "basic" && password == "" {
		return "", "", "", fmt.Errorf("basic-auth password is empty")
	}
	return token, username, password, nil
}

func sameOrigin(a, b *url.URL) bool {
	if a == nil || b == nil {
		return false
	}
	return strings.EqualFold(a.Scheme, b.Scheme) && strings.EqualFold(a.Host, b.Host)
}

func responseError(operation string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	text := strings.TrimSpace(string(body))
	if text == "" {
		return fmt.Errorf("%s failed with HTTP %d %s", operation, resp.StatusCode, resp.Status)
	}
	return fmt.Errorf("%s failed with HTTP %d: %s", operation, resp.StatusCode, text)
}
