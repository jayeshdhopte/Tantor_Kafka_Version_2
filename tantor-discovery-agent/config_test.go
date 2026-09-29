package main

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestExtractBootstrapServers(t *testing.T) {
	got := extractBootstrapServers("CONTROLLER://node1:9093,INTERNAL://broker2:9092,INTERNAL://broker1:9092")
	want := "broker1:9092,broker2:9092"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestPathWithinRootRejectsEscape(t *testing.T) {
	root := t.TempDir()
	if _, err := pathWithinRoot(root, filepath.Join(root, "..", "outside")); err == nil {
		t.Fatal("expected path escape to be rejected")
	}
}

func TestApplyConfigChangesPreservesComments(t *testing.T) {
	input := "# comment\nlisteners=PLAINTEXT://host:9092\nlog.dirs=/data/kafka\n"
	out := applyConfigChanges(input, map[string]string{
		"log.dirs":       "/data/new",
		"num.io.threads": "16",
	})
	if !strings.Contains(out, "# comment") || !strings.Contains(out, "log.dirs=/data/new") || !strings.Contains(out, "num.io.threads=16") {
		t.Fatalf("unexpected output: %s", out)
	}
}

func TestParsePropertiesSupportsContinuationAndEscapes(t *testing.T) {
	props := parseProperties("listeners=INTERNAL\\://host\\:9092,\\\n  EXTERNAL\\://host\\:9094\nlog\\.dirs=/data/kafka\n")
	if props["listeners"] != "INTERNAL://host:9092,EXTERNAL://host:9094" {
		t.Fatalf("unexpected listeners: %q", props["listeners"])
	}
	if props["log.dirs"] != "/data/kafka" {
		t.Fatalf("unexpected log.dirs: %q", props["log.dirs"])
	}
}

func TestUnknownListenerSecurityDoesNotDefaultToPlaintext(t *testing.T) {
	got := detectSecurityForBrokerListeners("CUSTOM://host:9092", "CUSTOM://host:9092", "")
	if got != "UNKNOWN" {
		t.Fatalf("got %q want UNKNOWN", got)
	}
}

func TestLoadRuntimeConfigRequiresProductionIdentityAndScanPaths(t *testing.T) {
	t.Setenv("TANTOR_AGENT_SERVER_URL", "http://127.0.0.1:8080")
	t.Setenv("TANTOR_AGENT_SCAN_PATHS", "")
	t.Setenv("TANTOR_AGENT_ENVIRONMENT", "prod")
	t.Setenv("TANTOR_AGENT_NODE_NAME", "node1")
	if _, err := LoadRuntimeConfig(nil); err == nil {
		t.Fatal("expected missing scan paths to fail")
	}
}

func TestLoadRuntimeConfigAcceptsSeparateControllerJMXEndpoint(t *testing.T) {
	t.Setenv("TANTOR_AGENT_SERVER_URL", "http://127.0.0.1:8080")
	t.Setenv("TANTOR_AGENT_SCAN_PATHS", t.TempDir())
	t.Setenv("TANTOR_AGENT_ENVIRONMENT", "prod")
	t.Setenv("TANTOR_AGENT_NODE_NAME", "node1")
	t.Setenv("TANTOR_AGENT_JMX_METRICS_URL", "http://127.0.0.1:8078/metrics")
	t.Setenv("TANTOR_AGENT_CONTROLLER_JMX_METRICS_URL", "http://127.0.0.1:8079/metrics")

	cfg, err := LoadRuntimeConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.JMXMetricsURL != "http://127.0.0.1:8078/metrics" {
		t.Fatalf("unexpected broker JMX endpoint %q", cfg.JMXMetricsURL)
	}
	if cfg.ControllerJMXMetricsURL != "http://127.0.0.1:8079/metrics" {
		t.Fatalf("unexpected controller JMX endpoint %q", cfg.ControllerJMXMetricsURL)
	}
}
