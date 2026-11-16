package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
)

func TestExternalReadinessTransportIsOptInAndRejectsMutation(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer fixture" {
			t.Error("missing bearer")
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/claim") {
			io.WriteString(w, `{"id":"fixture-task","claimToken":"fixture-claim","command":"RESTART_SERVICE","parameters":{}}`)
		} else if strings.HasSuffix(r.URL.Path, "/fixture-task/result") {
			var result map[string]any
			json.NewDecoder(r.Body).Decode(&result)
			if result["status"] != "FAILED" || result["claimToken"] != "fixture-claim" {
				t.Error("unsupported command was not rejected")
			}
			io.WriteString(w, `{}`)
		} else {
			t.Error("unexpected external task endpoint")
		}
	}))
	defer server.Close()
	client := &APIClient{baseURL: server.URL, http: server.Client(), authType: "bearer", token: "fixture", logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	cfg := RuntimeConfig{HostID: "candidate"}
	if err := pollAddNodeReadiness(context.Background(), client, cfg); err != nil || calls != 0 {
		t.Fatal("default agent contacted readiness API", err)
	}
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		t.Skip("active inspection requires Linux root")
	}
	cfg.EnableAddNodeInspection = true
	cfg.EnableTasks = true
	cfg.AuthType = "bearer"
	if err := pollAddNodeReadiness(context.Background(), client, cfg); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("unexpected calls %d", calls)
	}
}
