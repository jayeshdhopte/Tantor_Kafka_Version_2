package javaupgrade

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type failingControllerProbe struct{ *separateExecutor }

func (f failingControllerProbe) Run(ctx context.Context, cmd string, args ...string) (string, string, error) {
	if strings.HasSuffix(cmd, "kafka-metadata-quorum.sh") {
		return "", "Connection refused at controller listener", errors.New("exit status 1")
	}
	return f.separateExecutor.Run(ctx, cmd, args...)
}

func TestControllerProbeRetainsEndpointAndCommandDiagnostic(t *testing.T) {
	service := ServiceTarget{Name: "custom-controller.service", Role: "controller", KafkaHome: "/opt/kafka", Endpoint: "192.168.3.100:19093"}
	err := probeKafkaService(context.Background(), failingControllerProbe{&separateExecutor{}}, service)
	if err == nil || !strings.Contains(err.Error(), service.Endpoint) || !strings.Contains(err.Error(), "Connection refused") {
		t.Fatalf("missing actionable diagnostic: %v", err)
	}
	if !strings.Contains(err.Error(), "exit status 1") {
		t.Fatal(err)
	}
}

func pair() []ServiceTarget {
	return []ServiceTarget{
		{Name: "broker.service", Role: "broker", KafkaHome: "/opt/kafka", Endpoint: "127.0.0.1:9092"},
		{Name: "controller.service", Role: "controller", KafkaHome: "/opt/kafka", Endpoint: "127.0.0.1:9093"},
	}
}
func TestSeparateServiceLayout(t *testing.T) {
	services := pair()
	if err := validateSeparateServices(services, "broker.service"); err != nil {
		t.Fatal(err)
	}
	if services[0].Role != "controller" {
		t.Fatal("controller must start first")
	}
	for _, change := range []func([]ServiceTarget){func(s []ServiceTarget) { s[1].Role = "broker" }, func(s []ServiceTarget) { s[1].Name = s[0].Name }, func(s []ServiceTarget) { s[1].Role = "broker_controller" }, func(s []ServiceTarget) { s[1].Name = "../../other.service" }} {
		invalid := pair()
		change(invalid)
		if validateSeparateServices(invalid, "broker.service") == nil {
			t.Fatal("invalid pair accepted")
		}
	}
	if validateSeparateServices(pair(), "unrelated.service") == nil {
		t.Fatal("unbound primary accepted")
	}
	if validateSeparateServices(pair()[:1], "broker.service") == nil {
		t.Fatal("incomplete pair accepted")
	}
}
func TestSeparateJavaGuardRetainsUnrelatedProcessBlock(t *testing.T) {
	allowed := map[string]bool{"101": true, "102": true}
	if err := rejectUnrelatedJava("101 java\n102 java\n9 bash", allowed); err != nil {
		t.Fatal(err)
	}
	if err := rejectUnrelatedJava("101 java\n102 java\n1734 java", allowed); err == nil || !strings.Contains(err.Error(), "1734") {
		t.Fatal("unrelated Elasticsearch JVM must remain blocked")
	}
}
func TestSeparateTransitionOrderingAndOriginallyStoppedServices(t *testing.T) {
	services := pair()
	_ = validateSeparateServices(services, "broker.service")
	var calls []string
	run := func(_ context.Context, _ string, args ...string) (string, error) {
		calls = append(calls, args[0]+" "+args[2])
		return "", nil
	}
	_ = transitionServices(context.Background(), run, services, "stop")
	_ = transitionServices(context.Background(), run, services, "start")
	want := []string{"stop broker.service", "stop controller.service", "start controller.service", "start broker.service"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("%v", calls)
	}
	calls = nil
	_ = transitionServices(context.Background(), run, services[:1], "start")
	if !reflect.DeepEqual(calls, []string{"start controller.service"}) {
		t.Fatal("inactive broker must not be started")
	}
}
func TestSeparateStartFailureDoesNotStartBroker(t *testing.T) {
	services := pair()
	_ = validateSeparateServices(services, "broker.service")
	calls := 0
	err := transitionServices(context.Background(), func(context.Context, string, ...string) (string, error) {
		calls++
		return "", errors.New("start failed")
	}, services, "start")
	if err == nil || calls != 1 {
		t.Fatal("controller start failure must prevent broker start")
	}
}

func TestStoppedServiceRequiresStoppedProcessAndExpectedExit(t *testing.T) {
	for _, tc := range []struct {
		state, pid, result, code string
		ok                       bool
	}{
		{"inactive", "0", "success", "0", true},
		{"failed", "0", "exit-code", "143", true},
		{"failed", "123", "exit-code", "143", false},
		{"failed", "0", "timeout", "143", false},
		{"failed", "0", "exit-code", "1", false},
		{"deactivating", "0", "success", "0", false},
		{"active", "123", "success", "0", false},
	} {
		t.Run(tc.state+tc.pid+tc.result+tc.code, func(t *testing.T) {
			run := func(_ context.Context, _ string, args ...string) (string, error) {
				values := map[string]string{"--property=ActiveState": tc.state, "--property=MainPID": tc.pid, "--property=Result": tc.result, "--property=ExecMainStatus": tc.code}
				return values[args[1]], nil
			}
			if err := verifyStoppedService(context.Background(), run, "custom-controller.service"); (err == nil) != tc.ok {
				t.Fatalf("unexpected verification: %v", err)
			}
		})
	}
}
