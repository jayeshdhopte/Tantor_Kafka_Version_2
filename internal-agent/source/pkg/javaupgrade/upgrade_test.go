package javaupgrade

import (
	"context"
	"errors"
	"io.translab/tantor-agent/pkg/api"
	"strings"
	"testing"
	"time"
)

func TestVersionAndArtifactGuards(t *testing.T) {
	for _, pair := range []struct {
		old, target string
		allow       bool
	}{{"17.0.21", "21.0.13", true}, {"21.0.13", "25.0.5", true}, {"17.0.21", "25.0.5", false}, {"21.0.13", "17.0.21", false}, {"21.0.13", "21.0.13", false}} {
		if Allowed(pair.old, pair.target) != pair.allow {
			t.Fatalf("bad policy %v", pair)
		}
	}
	for _, base := range []string{"/etc/cron.d", "/srv/tantor/../../etc", "relative/path"} {
		if _, err := ArchivePath(base, "00000000-0000-4000-8000-000000000001"); err == nil {
			t.Fatalf("unsafe path accepted: %s", base)
		}
	}
	if _, err := ArchivePath("/srv/tantor/parcels", "../../java"); err == nil {
		t.Fatal("unsafe artifact identifier accepted")
	}
	if err := RunDiscoveredService(context.Background(), nil, &api.Task{Parameters: map[string]string{"service_name": "different"}}, &strings.Builder{}, "custom.service"); err == nil {
		t.Fatal("unbound service accepted")
	}
}

func TestServiceVerificationWaitsForKafkaHealth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	run := func(_ context.Context, command string, _ ...string) (string, error) {
		if command == "systemctl" {
			return "42", nil
		}
		return "/old-jdk/bin/java", nil
	}
	probes := 0
	probe := func(context.Context) error {
		probes++
		if probes == 1 {
			return errors.New("Kafka is still starting")
		}
		return nil
	}
	if err := waitForJavaService(ctx, run, "kafka.service", "/old-jdk/bin/java", probe); err != nil {
		t.Fatalf("restored service should be allowed to become healthy: %v", err)
	}
	if probes != 2 {
		t.Fatalf("expected health to be retried, got %d calls", probes)
	}
}

func TestServiceVerificationRejectsWrongJvm(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	run := func(_ context.Context, command string, _ ...string) (string, error) {
		if command == "systemctl" {
			return "42", nil
		}
		return "/different-jdk/bin/java", nil
	}
	probe := func(context.Context) error {
		t.Fatal("Kafka health must not bypass the JVM identity check")
		return nil
	}
	if err := waitForJavaService(ctx, run, "kafka.service", "/old-jdk/bin/java", probe); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wrong JVM must not verify successfully: %v", err)
	}
}

func TestVerificationTimeoutRetainsUnderlyingCause(t *testing.T) {
	for _, healthFailure := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		run := func(_ context.Context, cmd string, _ ...string) (string, error) {
			if cmd == "systemctl" {
				return "42", nil
			}
			if healthFailure {
				return "/target/bin/java", nil
			}
			return "/pinned/jdk/bin/java", nil
		}
		err := waitForJavaService(ctx, run, "custom-controller.service", "/target/bin/java", func(context.Context) error { return errors.New("quorum connection refused") })
		cancel()
		expected := "/pinned/jdk/bin/java"
		if healthFailure {
			expected = "quorum connection refused"
		}
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), expected) {
			t.Fatalf("lost verification cause: %v", err)
		}
	}
}
