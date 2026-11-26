package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
		if len(paths) != 2 || len(records) != 2 || r.KafkaClusterID == "" {
			return nil, fmt.Errorf("VM-wide Java upgrade requires one process or a separate broker/controller pair")
		}
		roles, units := map[string]bool{}, map[string]bool{}
		for _, peer := range records {
			role := ""
			if roleContains(peer.ProcessRoles, "broker") && !roleContains(peer.ProcessRoles, "controller") {
				role = "broker"
			}
			if roleContains(peer.ProcessRoles, "controller") && !roleContains(peer.ProcessRoles, "broker") {
				role = "controller"
			}
			if role == "" || roles[role] || units[peer.SystemdService] || peer.SystemdService == "" || peer.KafkaClusterID != r.KafkaClusterID || !strings.EqualFold(peer.KafkaMode, "kraft") {
				return nil, fmt.Errorf("separate Kafka services must have distinct roles and units in the same cluster")
			}
			roles[role], units[peer.SystemdService] = true, true
		}
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
	params := map[string]string{}
	for k, v := range p {
		params[k] = v
	}
	// Resolve every unit from local discovery, not from caller-supplied unit lists.
	targets := []javaupgrade.ServiceTarget{}
	for _, peer := range records {
		if err := validateRestartServiceSafety(peer.SystemdService, peer, cfg); err != nil {
			return "", err
		}
		binding, bindErr := exec.CommandContext(ctx, "systemctl", "show", peer.SystemdService, "--property=ExecStart", "--value").Output()
		cwd, cwdErr := exec.CommandContext(ctx, "systemctl", "show", peer.SystemdService, "--property=WorkingDirectory", "--value").Output()
		if bindErr != nil || cwdErr != nil || !serviceConfigBindingMatches(string(binding), strings.TrimSpace(string(cwd)), peer.PropsFile) {
			return "", fmt.Errorf("Kafka service binding changed for %s; refresh discovery", peer.SystemdService)
		}
		content, readErr := os.ReadFile(peer.PropsFile)
		if readErr != nil {
			return "", readErr
		}
		role := "controller"
		if roleContains(peer.ProcessRoles, "broker") {
			role = "broker"
			if roleContains(peer.ProcessRoles, "controller") {
				role = "broker_controller"
			}
		}
		endpoint, endpointErr := javaHealthEndpoint(parseProperties(string(content)), role)
		if endpointErr != nil {
			return "", endpointErr
		}
		targets = append(targets, javaupgrade.ServiceTarget{Name: peer.SystemdService, Role: role, KafkaHome: peer.InstallPath, Endpoint: endpoint})
		if peer.SystemdService == r.SystemdService {
			params["service_name"] = strings.TrimSuffix(peer.SystemdService, ".service")
			params["service_role"], params["kafka_home"], params["health_endpoint"] = role, peer.InstallPath, endpoint
		}
	}
	delete(params, "separate_services")
	if len(targets) == 2 {
		encoded, encodeErr := json.Marshal(targets)
		if encodeErr != nil {
			return "", encodeErr
		}
		params["separate_services"] = string(encoded)
	}
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
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			continue
		}
		// A listener bound to the node IP is not reachable on loopback.
		// Only wildcard binds should be probed through a loopback address.
		switch host {
		case "", "0.0.0.0":
			host = "127.0.0.1"
		case "::":
			host = "::1"
		}
		return net.JoinHostPort(host, port), nil
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
