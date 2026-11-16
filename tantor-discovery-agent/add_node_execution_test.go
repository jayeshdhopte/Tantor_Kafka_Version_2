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
			json.NewEncoder(w).Encode(map[string]any{"id": "task", "command": "INSTALL_KAFKA", "claimToken": "claim", "parameters": map[string]string{}, "expiresAt": time.Now().Add(time.Minute)})
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

func TestBrokerLeasePreventsExpiredOrCancelledExecution(t *testing.T) {
	calls := 0
	action := func(ctx context.Context) (string, error) {
		calls++
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("unbounded action")
		}
		return "proof", nil
	}
	for _, deadline := range []time.Time{{}, time.Now().Add(-time.Second)} {
		if _, err := withBrokerLease(context.Background(), deadline, action); err == nil {
			t.Fatal("expired lease accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := withBrokerLease(ctx, time.Now().Add(time.Minute), action); err == nil {
		t.Fatal("cancelled action accepted")
	}
	if calls != 0 {
		t.Fatal("action began without a live lease")
	}
	expires := time.Now().Add(time.Minute)
	_, err := withBrokerLease(context.Background(), expires, func(ctx context.Context) (string, error) {
		deadline, _ := ctx.Deadline()
		if !deadline.Equal(expires) {
			t.Fatal("server lease not enforced")
		}
		return action(ctx)
	})
	if err != nil || calls != 1 {
		t.Fatal("valid bounded action failed")
	}
}
