package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadGeneratesStableHostIDAndDefaultsAgentName(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "agent.yaml")
	dataDir := filepath.Join(tmp, "data")
	content := `agent:
  host_id: ""
  agent_name: ""
  server_url: "http://10.0.0.191:8080"
  poll_interval_seconds: 10
  heartbeat_interval_seconds: 30
paths:
  data_dir: "` + dataDir + `"
  log_dir: "` + filepath.Join(tmp, "logs") + `"
  artifacts_dir: "` + filepath.Join(tmp, "artifacts") + `"
auth:
  mode: "none"
http:
  request_timeout_seconds: 60
  artifact_timeout_seconds: 120
  dial_timeout_seconds: 5
  tls_handshake_timeout_seconds: 5
  idle_conn_timeout_seconds: 30
  use_environment_proxy: false
privilege:
  mode: "sudo"
  sudo_path: "/usr/bin/sudo"
`
	if err := os.WriteFile(cfgPath, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	first, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("first Load failed: %v", err)
	}
	if first.Agent.HostID == "" {
		t.Fatal("expected generated host ID")
	}
	if first.Agent.AgentName == "" {
		t.Fatal("expected default agent name")
	}
	second, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("second Load failed: %v", err)
	}
	if first.Agent.HostID != second.Agent.HostID {
		t.Fatalf("host ID was not stable: %q != %q", first.Agent.HostID, second.Agent.HostID)
	}
}

func TestEnvironmentOverridesServerURL(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "agent.yaml")
	content := `agent:
  server_url: "http://old.example:8080"
paths:
  data_dir: "` + filepath.Join(tmp, "data") + `"
  log_dir: "` + filepath.Join(tmp, "logs") + `"
  artifacts_dir: "` + filepath.Join(tmp, "artifacts") + `"
`
	if err := os.WriteFile(cfgPath, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TANTOR_SERVER_URL", "http://new.example:9090")
	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Agent.ServerURL != "http://new.example:9090" {
		t.Fatalf("unexpected override: %q", cfg.Agent.ServerURL)
	}
}

func TestUnknownConfigKeyFailsFast(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "agent.yaml")
	content := `agent:
  server_url: "http://127.0.0.1:8080"
  typo_server: "bad"
`
	if err := os.WriteFile(cfgPath, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(cfgPath); err == nil {
		t.Fatal("expected unknown key to fail")
	}
}

func TestServerURLRejectsEmbeddedCredentials(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "agent.yaml")
	content := `agent:
  server_url: "https://user:secret@management.internal:8443"
paths:
  data_dir: "` + filepath.Join(tmp, "data") + `"
  log_dir: "` + filepath.Join(tmp, "logs") + `"
  artifacts_dir: "` + filepath.Join(tmp, "artifacts") + `"
`
	if err := os.WriteFile(cfgPath, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(cfgPath); err == nil {
		t.Fatal("expected embedded URL credentials to fail validation")
	}
}
