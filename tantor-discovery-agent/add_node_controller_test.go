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
	"time"
)

func TestControllerTransportKeepsReadinessAndMutationOptInsSeparate(t *testing.T) {
	if validateAddNodeInspectionMode(RuntimeConfig{EnableControllerAddition: true}, 0, "linux") == nil {
		t.Fatal("controller execution bypassed inspection opt-in")
	}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer fixture" {
			t.Error("missing task authentication")
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/claim") {
			var claim map[string]bool
			json.NewDecoder(r.Body).Decode(&claim)
			if claim["executionEnabled"] {
				t.Error("broker opt-in enabled controller execution")
			}
			json.NewEncoder(w).Encode(map[string]any{"id": "task", "command": "RUNTIME", "claimToken": "token", "parameters": map[string]string{"runtime_inspection_request": "{}"}, "expiresAt": time.Now().Add(time.Minute)})
		} else {
			var result map[string]any
			json.NewDecoder(r.Body).Decode(&result)
			if result["status"] != "FAILED" {
				t.Error("malformed runtime request accepted")
			}
			io.WriteString(w, `{}`)
		}
	}))
	defer server.Close()
	client := &APIClient{baseURL: server.URL, http: server.Client(), authType: "bearer", token: "fixture", logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := pollExternalControllerTask(context.Background(), client, RuntimeConfig{}); err != nil || calls != 0 {
		t.Fatal("default discovery claimed controller work")
	}
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		t.Skip("active controller adapter requires Linux root")
	}
	cfg := RuntimeConfig{HostID: "candidate", AuthType: "bearer", EnableTasks: true, EnableAddNodeInspection: true, EnableAddNodeExecution: true}
	if err := pollExternalControllerTask(context.Background(), client, cfg); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("unexpected calls %d", calls)
	}
}
