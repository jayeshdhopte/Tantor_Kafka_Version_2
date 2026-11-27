package javaupgrade

import (
	"context"
	"errors"
	"fmt"
	"io.translab/tantor-agent/internal/taskvalidate"
	"path/filepath"
	"regexp"
	"strings"
)

func rejectUnrelatedJava(processes string, allowedPIDs map[string]bool) error {
	for _, line := range strings.Split(processes, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[1] == "java" && !allowedPIDs[fields[0]] {
			return fmt.Errorf("another Java process is running on this VM (PID %s)", fields[0])
		}
	}
	return nil
}

// Callers pass only services that were running before the upgrade. Inactive
// services must never be started as a side effect of upgrade or recovery.
func transitionServices(ctx context.Context, run func(context.Context, string, ...string) (string, error), services []ServiceTarget, action string) error {
	var failures []error
	for i := 0; i < len(services); i++ {
		index := i
		if action == "stop" {
			index = len(services) - 1 - i
		}
		if _, err := run(ctx, "systemctl", action, "--", services[index].Name); err != nil {
			failures = append(failures, fmt.Errorf("%s %s: %w", action, services[index].Name, err))
			if action == "start" {
				break
			}
		}
	}
	return errors.Join(failures...)
}

// ServiceTarget is a server-approved process on the same Kafka host.
type ServiceTarget struct {
	Name      string `json:"name"`
	Role      string `json:"role"`
	KafkaHome string `json:"kafka_home"`
	Endpoint  string `json:"health_endpoint"`
}

func validateServiceTarget(s ServiceTarget) error {
	if !regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.@:-]{0,199}\.service$`).MatchString(s.Name) {
		return fmt.Errorf("invalid Kafka service name")
	}
	if s.Role != "broker" && s.Role != "controller" && s.Role != "broker_controller" {
		return fmt.Errorf("unsupported Kafka service role")
	}
	if _, err := taskvalidate.ApprovedPath(s.KafkaHome); err != nil {
		return err
	}
	if _, err := taskvalidate.HostPort(s.Endpoint); err != nil {
		return err
	}
	return nil
}

func validateSeparateServices(services []ServiceTarget, primary string) error {
	if len(services) != 2 {
		return fmt.Errorf("separate mode requires exactly one broker and one controller")
	}
	found, roles, names := false, map[string]bool{}, map[string]bool{}
	for _, s := range services {
		if err := validateServiceTarget(s); err != nil {
			return err
		}
		if (s.Role != "broker" && s.Role != "controller") || roles[s.Role] || names[s.Name] {
			return fmt.Errorf("separate mode requires distinct broker and controller services")
		}
		found = found || s.Name == primary
		roles[s.Role], names[s.Name] = true, true
	}
	if !found {
		return fmt.Errorf("primary Kafka service is missing from separate layout")
	}
	// Controller starts first; the stop loop traverses this order in reverse.
	if services[0].Role == "broker" {
		services[0], services[1] = services[1], services[0]
	}
	return nil
}

func probeKafkaService(ctx context.Context, executor Executor, s ServiceTarget) error {
	if s.Role != "controller" {
		out, stderr, err := executor.Run(ctx, filepath.Join(s.KafkaHome, "bin", "kafka-topics.sh"), "--bootstrap-server", s.Endpoint, "--describe", "--under-replicated-partitions")
		if err != nil || strings.TrimSpace(out) != "" {
			return probeFailure("Kafka partition", s.Endpoint, out, stderr, err)
		}
	}
	flag := "--bootstrap-server"
	if s.Role == "controller" {
		flag = "--bootstrap-controller"
	}
	out, stderr, err := executor.Run(ctx, filepath.Join(s.KafkaHome, "bin", "kafka-metadata-quorum.sh"), flag, s.Endpoint, "describe", "--status")
	if err != nil || !regexp.MustCompile(`(?m)^LeaderId:\s+[0-9]+\s*$`).MatchString(out) || !strings.Contains(out, "CurrentVoters:") {
		return probeFailure("KRaft quorum", s.Endpoint, out, stderr, err)
	}
	return nil
}

func probeFailure(check, endpoint, out, stderr string, err error) error {
	detail := strings.TrimSpace(stderr + "\n" + out)
	if len(detail) > 2048 {
		detail = detail[:2048] + " [truncated]"
	}
	if err == nil {
		err = fmt.Errorf("unexpected health response")
	}
	return fmt.Errorf("%s health was not verified at %s: %w; output: %s", check, endpoint, err, detail)
}

// Kafka launch scripts can report SIGTERM as exit 143: systemd marks the
// unit failed even though its process stopped. Do not accept other failures.
func verifyStoppedService(ctx context.Context, run func(context.Context, string, ...string) (string, error), name string) error {
	state, err := run(ctx, "systemctl", "show", "--property=ActiveState", "--value", "--", name)
	if err == nil && state == "inactive" {
		return nil
	}
	if err == nil && state == "failed" {
		pid, pe := run(ctx, "systemctl", "show", "--property=MainPID", "--value", "--", name)
		result, re := run(ctx, "systemctl", "show", "--property=Result", "--value", "--", name)
		code, ce := run(ctx, "systemctl", "show", "--property=ExecMainStatus", "--value", "--", name)
		if pe == nil && re == nil && ce == nil && pid == "0" && result == "exit-code" && code == "143" {
			return nil
		}
		return fmt.Errorf("Kafka service %s stop was not verified: state=%s MainPID=%s result=%s exit=%s", name, state, pid, result, code)
	}
	return fmt.Errorf("Kafka service %s did not stop before switching Java (state=%s, query error=%v)", name, state, err)
}
