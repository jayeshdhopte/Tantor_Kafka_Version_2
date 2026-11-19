package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io.translab/tantor-agent/pkg/api"
	"io.translab/tantor-agent/pkg/javaupgrade"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

func validateJavaMode(cfg RuntimeConfig) error {
	if !cfg.EnableTasks || cfg.AuthType != "bearer" || !cfg.AllowConfigWrite || os.Geteuid() != 0 {
		return fmt.Errorf("Java tasks require root, bearer authentication, tasks and configuration-write permission")
	}
	return nil
}

func javaTargetParameters(cfg RuntimeConfig, r DiscoveredCluster, records []DiscoveredCluster, t managedTask) (map[string]string, error) {
	if err := validateJavaMode(cfg); err != nil {
		return nil, err
	}
	if !strings.EqualFold(r.KafkaMode, "kraft") || r.SystemdService == "" {
		return nil, fmt.Errorf("a discovered systemd-managed KRaft process is required")
	}
	paths := map[string]bool{}
	for _, record := range records {
		paths[canonicalPathOrClean(record.PropsFile)] = true
	}
	if len(paths) != 1 {
		return nil, fmt.Errorf("VM-wide Java upgrade requires one discovered Kafka process on this dedicated host")
	}
	if err := validateDiscoveredService(t.Parameters["service_name"], r); err != nil {
		return nil, err
	}
	if _, err := javaupgrade.ArchivePath(t.Parameters["parcel_dir"], t.Parameters["artifact_id"]); err != nil {
		return nil, err
	}
	if !regexp.MustCompile(`^[a-fA-F0-9]{64}$`).MatchString(t.Parameters["checksum"]) {
		return nil, fmt.Errorf("approved JDK checksum required")
	}
	if !regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`).MatchString(t.Parameters["target_version"]) {
		return nil, fmt.Errorf("exact JDK version required")
	}
	return t.Parameters, nil
}

func runExternalJavaTask(ctx context.Context, cfg RuntimeConfig, r DiscoveredCluster, records []DiscoveredCluster, t managedTask) (string, error) {
	p, err := javaTargetParameters(cfg, r, records, t)
	if err != nil {
		return "", err
	}
	archive, _ := javaupgrade.ArchivePath(p["parcel_dir"], p["artifact_id"])
	if t.Command == "REMOVE_JAVA" {
		if err := os.Remove(archive); err != nil && !os.IsNotExist(err) {
			return "", err
		}
		return "Removed staged JDK archive only; installed Java unchanged", nil
	}
	if t.Command != "JAVA_UPGRADE" {
		return "", fmt.Errorf("unsupported Java task")
	}
	if !cfg.AllowServiceRestart {
		return "", fmt.Errorf("service restart permission required")
	}
	if err := validateRestartServiceSafety(r.SystemdService, r, cfg); err != nil {
		return "", err
	}
	binding, err := exec.CommandContext(ctx, "systemctl", "show", r.SystemdService, "--property=ExecStart", "--value").Output()
	cwd, cwdErr := exec.CommandContext(ctx, "systemctl", "show", r.SystemdService, "--property=WorkingDirectory", "--value").Output()
	if err != nil || cwdErr != nil || !serviceConfigBindingMatches(string(binding), strings.TrimSpace(string(cwd)), r.PropsFile) {
		return "", fmt.Errorf("Kafka service binding changed; refresh discovery")
	}
	content, err := os.ReadFile(r.PropsFile)
	if err != nil {
		return "", err
	}
	props := parseProperties(string(content))
	role := "controller"
	if roleContains(r.ProcessRoles, "broker") {
		role = "broker"
		if roleContains(r.ProcessRoles, "controller") {
			role = "broker_controller"
		}
	}
	endpoint, err := javaHealthEndpoint(props, role)
	if err != nil {
		return "", err
	}
	params := map[string]string{}
	for k, v := range p {
		params[k] = v
	}
	params["service_name"] = strings.TrimSuffix(r.SystemdService, ".service")
	params["service_role"] = role
	params["kafka_home"] = r.InstallPath
	params["health_endpoint"] = endpoint
	var logs strings.Builder
	err = javaupgrade.RunDiscoveredService(ctx, serviceExecutor{}, &api.Task{TaskID: t.ID, Parameters: params, Checksum: p["checksum"]}, &logs, r.SystemdService)
	return logs.String(), err
}

func javaHealthEndpoint(props map[string]string, role string) (string, error) {
	controllers := "," + strings.ToUpper(props["controller.listener.names"]) + ","
	protocols := map[string]string{}
	for _, part := range strings.Split(props["listener.security.protocol.map"], ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), ":")
		if ok {
			protocols[strings.ToUpper(k)] = strings.ToUpper(v)
		}
	}
	for _, listener := range strings.Split(props["listeners"], ",") {
		name, address, ok := strings.Cut(strings.TrimSpace(listener), "://")
		if !ok {
			continue
		}
		name = strings.ToUpper(name)
		controller := strings.Contains(controllers, ","+name+",")
		if (role == "controller") != controller {
			continue
		}
		protocol := protocols[name]
		if protocol == "" {
			protocol = name
		}
		if protocol != "PLAINTEXT" {
			continue
		}
		_, port, err := net.SplitHostPort(address)
		if err != nil {
			continue
		}
		return net.JoinHostPort("127.0.0.1", port), nil
	}
	return "", fmt.Errorf("a local PLAINTEXT listener for the Kafka role is required")
}

func executeExternalJavaStage(ctx context.Context, client *APIClient, cfg RuntimeConfig, records []DiscoveredCluster, t managedTask) serviceResult {
	managedMutationMu.Lock()
	defer managedMutationMu.Unlock()
	fail := func(err error) serviceResult { return serviceResult{"FAILED", err.Error()} }
	r, err := managedTarget(t, records)
	if err != nil {
		return fail(err)
	}
	p, err := javaTargetParameters(cfg, r, records, t)
	if err != nil {
		return fail(err)
	}
	archive, _ := javaupgrade.ArchivePath(p["parcel_dir"], p["artifact_id"])
	directory := filepath.Dir(archive)
	if err = os.MkdirAll(directory, 0755); err != nil {
		return fail(err)
	}
	// Never stage through symlinked directories.
	resolved, err := filepath.EvalSymlinks(directory)
	if err != nil || resolved != directory {
		return fail(fmt.Errorf("parcel directory must not contain symlinks"))
	}
	file, err := os.CreateTemp(directory, ".jdk-download-*")
	if err != nil {
		return fail(err)
	}
	defer os.Remove(file.Name())
	defer file.Close()
	// Construct the URL locally: a task cannot redirect the agent to another server.
	url := client.baseURL + "/api/v1/artifacts/" + p["artifact_id"] + "/download"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fail(err)
	}
	client.applyAuth(request)
	download := *client.http
	download.Timeout = 10 * time.Minute
	download.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := download.Do(request)
	if err != nil {
		return fail(fmt.Errorf("JDK download failed"))
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return fail(fmt.Errorf("JDK download HTTP %d", response.StatusCode))
	}
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(file, hash), io.LimitReader(response.Body, (1<<30)+1))
	if err != nil {
		return fail(err)
	}
	if n > 1<<30 {
		return fail(fmt.Errorf("JDK archive exceeds 1 GiB"))
	}
	if !strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), p["checksum"]) {
		return fail(fmt.Errorf("JDK checksum mismatch"))
	}
	if err = file.Close(); err != nil {
		return fail(err)
	}
	if err = javaupgrade.InspectArchive(file.Name(), p["target_version"]); err != nil {
		return fail(err)
	}
	if err = os.Rename(file.Name(), archive); err != nil {
		return fail(err)
	}
	return serviceResult{"SUCCESS", "JDK checksum, architecture and version verified; archive staged. Java is unchanged."}
}
