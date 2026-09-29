package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const maxAPIResponseBytes = 4 << 20

type APIClient struct {
	baseURL  string
	http     *http.Client
	authType string
	username string
	password string
	token    string
	retries  int
	logger   *slog.Logger
}

type APIError struct {
	StatusCode int
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("backend returned HTTP %d: %s", e.StatusCode, e.Body)
}

func readSecretFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	secret := strings.TrimSpace(string(b))
	if secret == "" {
		return "", errors.New("secret file is empty")
	}
	return secret, nil
}

func NewAPIClient(cfg RuntimeConfig, logger *slog.Logger) (*APIClient, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: cfg.InsecureSkipVerify} // #nosec G402 -- explicit opt-in compatibility flag

	if cfg.CAFile != "" {
		pemData, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if ok := pool.AppendCertsFromPEM(pemData); !ok {
			return nil, fmt.Errorf("CA file %s contains no valid PEM certificates", cfg.CAFile)
		}
		tlsConfig.RootCAs = pool
	}

	if cfg.ClientCertFile != "" || cfg.ClientKeyFile != "" {
		if cfg.ClientCertFile == "" || cfg.ClientKeyFile == "" {
			return nil, errors.New("both client certificate and client key are required")
		}
		cert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load mTLS client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	transport := &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       tlsConfig,
	}

	client := &APIClient{
		baseURL:  strings.TrimRight(cfg.ServerURL, "/"),
		http:     &http.Client{Transport: transport, Timeout: cfg.HTTPTimeout},
		authType: cfg.AuthType,
		username: cfg.AuthUsername,
		password: cfg.AuthPassword,
		token:    cfg.AuthToken,
		retries:  cfg.HTTPRetries,
		logger:   logger,
	}

	if cfg.AuthPasswordFile != "" {
		secret, err := readSecretFile(cfg.AuthPasswordFile)
		if err != nil {
			return nil, fmt.Errorf("read auth password file: %w", err)
		}
		client.password = secret
	}
	if cfg.AuthTokenFile != "" {
		secret, err := readSecretFile(cfg.AuthTokenFile)
		if err != nil {
			return nil, fmt.Errorf("read auth token file: %w", err)
		}
		client.token = secret
	}
	return client, nil
}

func (c *APIClient) endpoint(path string) string {
	return c.baseURL + "/" + strings.TrimLeft(path, "/")
}

func (c *APIClient) applyAuth(req *http.Request) {
	switch c.authType {
	case "bearer":
		req.Header.Set("Authorization", "Bearer "+c.token)
	case "basic":
		req.SetBasicAuth(c.username, c.password)
	}
}

func shouldRetryStatus(code int) bool {
	return code == http.StatusTooManyRequests || code == http.StatusInternalServerError || code == http.StatusBadGateway || code == http.StatusServiceUnavailable || code == http.StatusGatewayTimeout
}

func retryDelay(attempt int, retryAfter string) time.Duration {
	if retryAfter != "" {
		if seconds, err := strconv.Atoi(strings.TrimSpace(retryAfter)); err == nil && seconds >= 0 && seconds <= 60 {
			return time.Duration(seconds) * time.Second
		}
	}
	d := time.Duration(1<<min(attempt, 5)) * 250 * time.Millisecond
	if d > 8*time.Second {
		return 8 * time.Second
	}
	return d
}

func (c *APIClient) DoJSON(ctx context.Context, method, endpoint string, query url.Values, requestBody any, responseBody any) (int, error) {
	var bodyBytes []byte
	var err error
	if requestBody != nil {
		bodyBytes, err = json.Marshal(requestBody)
		if err != nil {
			return 0, fmt.Errorf("marshal request: %w", err)
		}
	}

	requestURL := endpoint
	if len(query) > 0 {
		requestURL += "?" + query.Encode()
	}

	for attempt := 0; attempt <= c.retries; attempt++ {
		var body io.Reader
		if bodyBytes != nil {
			body = bytes.NewReader(bodyBytes)
		}
		req, err := http.NewRequestWithContext(ctx, method, requestURL, body)
		if err != nil {
			return 0, err
		}
		req.Header.Set("Accept", "application/json")
		if bodyBytes != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		req.Header.Set("User-Agent", "tantor-discovery-agent/production")
		c.applyAuth(req)

		resp, err := c.http.Do(req)
		if err != nil {
			if attempt < c.retries && ctx.Err() == nil {
				delay := retryDelay(attempt, "")
				c.logger.Warn("backend request failed; retrying", "method", method, "url", requestURL, "attempt", attempt+1, "delay", delay, "error", err)
				if !sleepContext(ctx, delay) {
					return 0, ctx.Err()
				}
				continue
			}
			return 0, err
		}

		responseBytes, readErr := io.ReadAll(io.LimitReader(resp.Body, maxAPIResponseBytes))
		_ = resp.Body.Close()
		if readErr != nil {
			return resp.StatusCode, readErr
		}

		if shouldRetryStatus(resp.StatusCode) && attempt < c.retries {
			delay := retryDelay(attempt, resp.Header.Get("Retry-After"))
			c.logger.Warn("backend returned temporary failure; retrying", "status", resp.StatusCode, "url", requestURL, "attempt", attempt+1, "delay", delay)
			if !sleepContext(ctx, delay) {
				return resp.StatusCode, ctx.Err()
			}
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			bodyText := strings.TrimSpace(string(responseBytes))
			if len(bodyText) > 1024 {
				bodyText = bodyText[:1024] + "..."
			}
			return resp.StatusCode, &APIError{StatusCode: resp.StatusCode, Body: bodyText}
		}
		if responseBody != nil && len(bytes.TrimSpace(responseBytes)) > 0 {
			if err := json.Unmarshal(responseBytes, responseBody); err != nil {
				return resp.StatusCode, fmt.Errorf("decode response: %w", err)
			}
		}
		return resp.StatusCode, nil
	}
	return 0, errors.New("request failed after retries")
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
