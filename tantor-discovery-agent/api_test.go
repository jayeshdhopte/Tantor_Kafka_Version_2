package main

import "testing"

func TestConfiguredJMXExporterPort(t *testing.T) {
	tests := []struct {
		name       string
		metricsURL string
		want       int
		wantNil    bool
	}{
		{name: "IPv4", metricsURL: "http://192.168.3.164:7080/metrics", want: 7080},
		{name: "host template", metricsURL: "http://{host}:17071/metrics", want: 17071},
		{name: "IPv6", metricsURL: "http://[2001:db8::1]:27071/metrics", want: 27071},
		{name: "not configured", wantNil: true},
		{name: "no explicit port", metricsURL: "https://metrics.example/metrics", wantNil: true},
		{name: "invalid", metricsURL: "://bad", wantNil: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := configuredJMXExporterPort(test.metricsURL)
			if test.wantNil {
				if got != nil {
					t.Fatalf("configuredJMXExporterPort(%q) = %d, want nil", test.metricsURL, *got)
				}
				return
			}
			if got == nil || *got != test.want {
				t.Fatalf("configuredJMXExporterPort(%q) = %v, want %d", test.metricsURL, got, test.want)
			}
		})
	}
}

func TestJMXMetricsURLForClusterUsesRoleSpecificEndpoint(t *testing.T) {
	cfg := RuntimeConfig{
		JMXMetricsURL:           "http://{host}:8078/metrics",
		ControllerJMXMetricsURL: "http://{host}:8079/metrics",
	}
	tests := []struct {
		name  string
		roles string
		want  string
	}{
		{name: "broker", roles: "broker", want: cfg.JMXMetricsURL},
		{name: "controller", roles: "controller", want: cfg.ControllerJMXMetricsURL},
		{name: "combined process", roles: "broker,controller", want: cfg.JMXMetricsURL},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := jmxMetricsURLForCluster(cfg, DiscoveredCluster{ProcessRoles: test.roles})
			if got != test.want {
				t.Fatalf("jmxMetricsURLForCluster(%q) = %q, want %q", test.roles, got, test.want)
			}
		})
	}
}

func TestJMXMetricsURLForClusterDoesNotReuseBrokerEndpointForController(t *testing.T) {
	cfg := RuntimeConfig{JMXMetricsURL: "http://{host}:8078/metrics"}
	got := jmxMetricsURLForCluster(cfg, DiscoveredCluster{ProcessRoles: "controller"})
	if got != "" {
		t.Fatalf("controller endpoint = %q, want empty when not configured", got)
	}
}
