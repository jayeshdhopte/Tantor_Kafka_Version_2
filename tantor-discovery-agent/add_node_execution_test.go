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

func TestBrokerExecutionRequiresSeparateOptIn(t *testing.T) {
	cfg := RuntimeConfig{EnableAddNodeExecution: true}
	if validateAddNodeInspectionMode(cfg, 0, "linux") == nil {
		t.Fatal("execution bypassed inspection opt-in")
	}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") != "Bearer fixture" {
			t.Error("missing auth")
		}
		if strings.HasSuffix(r.URL.Path, "/claim") {
			io.WriteString(w, `{"id":"task","command":"INSTALL_KAFKA","claimToken":"claim","parameters":{}}`)
		} else {
			var result map[string]any
			json.NewDecoder(r.Body).Decode(&result)
			if result["status"] != "FAILED" {
				t.Error("arbitrary deployment was accepted")
			}
			io.WriteString(w, `{}`)
		}
	}))
	defer server.Close()
	client := &APIClient{baseURL: server.URL, http: server.Client(), authType: "bearer", token: "fixture", logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := pollExternalBrokerTask(context.Background(), client, RuntimeConfig{}); err != nil || calls != 0 {
		t.Fatal("default discovery enabled broker actions")
	}
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		t.Skip("active adapter requires Linux root")
	}
	cfg = RuntimeConfig{HostID: "candidate", AuthType: "bearer", EnableTasks: true, EnableAddNodeInspection: true, EnableAddNodeExecution: true}
	if err := pollExternalBrokerTask(context.Background(), client, cfg); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("unexpected calls %d", calls)
	}
}
