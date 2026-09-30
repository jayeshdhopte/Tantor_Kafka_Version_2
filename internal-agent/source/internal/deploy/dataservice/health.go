package dataservice

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func Verify(ctx context.Context, kind, base string) (string, error) {
	// JVM startup can take longer than a minute after a fresh archive install.
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	parsed, err := url.Parse(base)
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return "", fmt.Errorf("a PLAINTEXT HTTP service URL without credentials or path is required")
	}
	client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	routes := []string{"/subjects"}
	if kind == Connect {
		routes = []string{"/", "/connectors"}
	}
	var lastErr error
	for {
		lastErr = nil
		for _, route := range routes {
			req, _ := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(base, "/")+route, nil)
			resp, e := client.Do(req)
			if e != nil {
				lastErr = e
				break
			}
			body, e := io.ReadAll(io.LimitReader(resp.Body, 1024*1024+1))
			resp.Body.Close()
			if e != nil || len(body) > 1024*1024 || resp.StatusCode != http.StatusOK {
				lastErr = fmt.Errorf("%s returned HTTP %d or unreadable body", route, resp.StatusCode)
				break
			}
			if route == "/" {
				var worker struct {
					Version string `json:"version"`
				}
				if json.Unmarshal(body, &worker) != nil || worker.Version == "" {
					lastErr = fmt.Errorf("Connect root response lacks a version")
					break
				}
			} else {
				var items []string
				if json.Unmarshal(body, &items) != nil || items == nil {
					lastErr = fmt.Errorf("%s did not return a JSON array", route)
					break
				}
			}
		}
		if lastErr == nil {
			return "[PASS] " + kind + " REST API verified at " + base, nil
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("%s verification failed: %v (%w)", kind, lastErr, ctx.Err())
		case <-time.After(time.Second):
		}
	}
}
