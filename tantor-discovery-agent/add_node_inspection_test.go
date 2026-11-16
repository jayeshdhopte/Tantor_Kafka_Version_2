package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExternalAddNodeInspectionIsExplicitAndCannotEnableMutation(t *testing.T) {
	if len(buildVersion) > 128 || !strings.HasSuffix(buildVersion, "-an.1") {
		t.Fatal("external version must fit the deployed VARCHAR(128) and retain the inspection marker")
	}
	cfg := RuntimeConfig{}
	if err := validateAddNodeInspectionMode(cfg, 1000, "windows"); err != nil {
		t.Fatal("default discovery changed", err)
	}
	cfg.EnableAddNodeInspection = true
	for _, test := range []struct {
		tasks bool
		auth  string
		uid   int
		goos  string
	}{{false, "bearer", 0, "linux"}, {true, "none", 0, "linux"}, {true, "bearer", 1000, "linux"}, {true, "bearer", 0, "windows"}} {
		cfg.EnableTasks = test.tasks
		cfg.AuthType = test.auth
		if err := validateAddNodeInspectionMode(cfg, test.uid, test.goos); err == nil {
			t.Fatal("unsafe inspection mode accepted", test)
		}
	}
	cfg.EnableTasks = true
	cfg.AuthType = "bearer"
	if err := validateAddNodeInspectionMode(cfg, 0, "linux"); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(addNodeHostObservation{SchemaVersion: externalAddNodeInspectionSchema})
	if !strings.Contains(string(b), `"executionEnabled":false`) {
		t.Fatal("execution must be explicitly false")
	}
}

func TestExternalHostInventoryObservesProcessesOutsideDiscoveryAndExcludesDBus(t *testing.T) {
	root := t.TempDir()
	proc := filepath.Join(root, "proc")
	units := filepath.Join(root, "units")
	os.MkdirAll(filepath.Join(proc, "42"), 0755)
	os.MkdirAll(units, 0755)
	os.WriteFile(filepath.Join(proc, "42", "cmdline"), []byte("java\x00-cp\x00/foreign/libs/*\x00kafka.Kafka\x00/foreign/config.properties\x00"), 0600)
	os.WriteFile(filepath.Join(units, "dbus-broker.service"), []byte("unrelated"), 0600)
	complete, p, u := inspectAddNodeInventory(proc, []string{units})
	if !complete || p != 1 || u != 0 {
		t.Fatalf("unexpected inventory complete=%v processes=%d units=%d", complete, p, u)
	}
	os.WriteFile(filepath.Join(units, "controller.service"), []byte("stale or active"), 0600)
	complete, p, u = inspectAddNodeInventory(proc, []string{units})
	if !complete || p != 1 || u != 1 {
		t.Fatal("Kafka unit not observed")
	}
	if ok, _, _ := inspectAddNodeInventory(filepath.Join(root, "missing"), []string{units}); ok {
		t.Fatal("unreadable inventory reported complete")
	}
	if ok, _, _ := inspectAddNodeInventory(proc, []string{filepath.Join(root, "missing")}); ok {
		t.Fatal("missing systemd inventory reported complete")
	}
}

func TestExternalFreshHostInspectionPublishesHeartbeatWithoutKafkaDiscovery(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("opt-in publisher is root-only")
	}
	calls := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.URL.Path)
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing authenticated reporting")
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if r.URL.Path == externalAddNodeInspectionAPI {
			if body["agentId"] != "external-fresh" || body["executionEnabled"] != false || body["schemaVersion"] != externalAddNodeInspectionSchema {
				t.Error("wrong host inspection envelope")
			}
		} else if r.URL.Path != "/api/v1/ui/external-clusters/discovery/heartbeat" {
			t.Error("fresh observation registered a Kafka cluster")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{}`)
	}))
	defer server.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := &APIClient{baseURL: server.URL, http: server.Client(), authType: "bearer", token: "test-token", logger: logger}
	cfg := RuntimeConfig{EnableTasks: true, EnableAddNodeInspection: true, AuthType: "bearer", HostID: "external-fresh", NodeName: "fresh-node"}
	if err := publishAddNodeInspection(context.Background(), client, cfg, nil); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[0] != "/api/v1/ui/external-clusters/discovery/heartbeat" || calls[1] != externalAddNodeInspectionAPI {
		t.Fatalf("unexpected requests %v", calls)
	}
	cfg.EnableAddNodeInspection = false
	calls = nil
	if err := publishAddNodeInspection(context.Background(), client, cfg, nil); err != nil || len(calls) != 0 {
		t.Fatal("disabled inspection contacted backend")
	}
}

func TestAddNodeInspectionConfigFlagDefaultsOffAndCanBeExplicitlyEnabled(t *testing.T) {
	t.Setenv("TANTOR_AGENT_SERVER_URL", "http://127.0.0.1:8443")
	t.Setenv("TANTOR_AGENT_ENVIRONMENT", "prod")
	t.Setenv("TANTOR_AGENT_NODE_NAME", "test-node")
	t.Setenv("TANTOR_AGENT_SCAN_PATHS", t.TempDir())
	t.Setenv("TANTOR_AGENT_ENABLE_ADD_NODE_INSPECTION", "false")
	cfg, err := LoadRuntimeConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.EnableAddNodeInspection {
		t.Fatal("inspection default enabled")
	}
	cfg, err = LoadRuntimeConfig([]string{"--enable-add-node-inspection"})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.EnableAddNodeInspection {
		t.Fatal("explicit inspection flag ignored")
	}
}
