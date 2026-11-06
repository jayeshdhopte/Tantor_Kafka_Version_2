//go:build linux

package expansion

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"io.translab/tantor-agent/internal/deploy/processinspect"
	"io.translab/tantor-agent/internal/executor"
)

type RuntimeInspectionRequest struct {
	SchemaVersion          string                 `json:"schemaVersion"`
	Identity               processinspect.Request `json:"identity"`
	ParcelDir              string                 `json:"parcelDir"`
	RuntimeHome            string                 `json:"runtimeHome"`
	ArtifactSHA256         string                 `json:"artifactSha256"`
	ApprovedAgentChecksums []string               `json:"approvedAgentChecksums"`
}
type RuntimeInspectionReport struct {
	SchemaVersion  string                `json:"schemaVersion"`
	Identity       processinspect.Report `json:"identity"`
	Runtime        RuntimeProof          `json:"runtime"`
	JavaAgents     map[string]string     `json:"javaAgents"`
	ResolvedPaths  map[string]string     `json:"resolvedPaths"`
	Inherited      map[string]string     `json:"inherited"`
	ControllerPort int                   `json:"controllerPort"`
	ObservedAt     time.Time             `json:"observedAt"`
}

// InspectRuntime adds approved artifact evidence to the existing immutable
// identity inspector. No service or configuration changes are made.
func InspectRuntime(ctx context.Context, raw, hostID string, e executor.Executor) (string, error) {
	if len(raw) > 65536 {
		return "", fmt.Errorf("bounded runtime inspection required")
	}
	var r RuntimeInspectionRequest
	if json.Unmarshal([]byte(raw), &r) != nil || r.SchemaVersion != "add-node-runtime-inspection/v1" || !safePath(r.ParcelDir) || r.RuntimeHome != "" && !safePath(r.RuntimeHome) || !hash.MatchString(r.ArtifactSHA256) || len(r.ApprovedAgentChecksums) > 8 {
		return "", fmt.Errorf("invalid approved runtime inspection")
	}
	for _, checksum := range r.ApprovedAgentChecksums {
		if !hash.MatchString(checksum) {
			return "", fmt.Errorf("invalid Java agent catalog approval")
		}
	}
	identityRaw, err := json.Marshal(r.Identity)
	if err != nil {
		return "", err
	}
	if _, err = processinspect.Validate(string(identityRaw), hostID); err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	bindings := map[string]string{}
	read := func(name string, max int64) ([]byte, error) {
		if strings.HasPrefix(name, "/proc/") {
			raw, err := readBounded(ctx, name, max)
			return []byte(raw), err
		}
		resolved, err := resolveObservationPath(name, r.Identity.ConfigPath, r.Identity.ServiceName)
		if err != nil {
			return nil, err
		}
		if previous, ok := bindings[name]; ok && previous != resolved {
			return nil, fmt.Errorf("observed installation alias changed")
		}
		if len(bindings) >= 32 {
			if _, ok := bindings[name]; !ok {
				return nil, fmt.Errorf("too many observed path bindings")
			}
		}
		bindings[name] = resolved
		raw, err := readBounded(ctx, resolved, max)
		if err != nil {
			return nil, err
		}
		again, err := resolveObservationPath(name, r.Identity.ConfigPath, r.Identity.ServiceName)
		if err != nil || again != resolved {
			return nil, fmt.Errorf("observed path changed while reading")
		}
		return []byte(raw), nil
	}
	first, err := processinspect.RunWithReader(ctx, string(identityRaw), hostID, e, read)
	if err != nil || !first.IdentityVerified {
		return "", fmt.Errorf("participant identity is not verified")
	}
	manifest, err := loadManifest(ctx, Plan{ParcelDir: r.ParcelDir, ArtifactSHA256: r.ArtifactSHA256, KafkaVersion: r.Identity.KafkaVersion})
	if err != nil {
		return "", err
	}
	command, err := readBounded(ctx, filepath.Join("/proc", strconv.Itoa(first.PID), "cmdline"), 65536)
	if err != nil {
		return "", err
	}
	args := strings.Split(strings.TrimSuffix(command, "\x00"), "\x00")
	if r.RuntimeHome == "" {
		for i, arg := range args {
			if (arg == "-cp" || arg == "-classpath" || arg == "--class-path") && i+1 < len(args) {
				entry := strings.Split(args[i+1], ":")[0]
				wildcard := strings.HasSuffix(entry, "/*")
				if wildcard {
					entry = strings.TrimSuffix(entry, "/*")
				}
				resolved, err := (diskRuntimeFiles{}).Canonical(ctx, entry)
				if err != nil {
					return "", err
				}
				libs := filepath.Dir(resolved)
				if wildcard {
					libs = resolved
				}
				if filepath.Base(libs) != "libs" {
					return "", fmt.Errorf("installed Kafka runtime root cannot be established")
				}
				r.RuntimeHome = filepath.Dir(libs)
				break
			}
		}
		if !safePath(r.RuntimeHome) {
			return "", fmt.Errorf("installed Kafka runtime root unavailable")
		}
	}
	agents := map[string]string{}
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-javaagent:") {
			continue
		}
		file, err := (diskRuntimeFiles{}).Canonical(ctx, strings.SplitN(strings.TrimPrefix(arg, "-javaagent:"), "=", 2)[0])
		if err != nil {
			return "", err
		}
		if !safePath(file) {
			return "", fmt.Errorf("Java agent path is outside the supported adapter")
		}
		sum, err := fileSHA256(ctx, file, 128*1024*1024)
		if err != nil {
			return "", err
		}
		approved := false
		for _, expected := range r.ApprovedAgentChecksums {
			approved = approved || expected == sum
		}
		if !approved {
			return "", fmt.Errorf("running Java agent differs from the approved catalog")
		}
		agents[file] = sum
	}
	if err = verifyRuntimePayload(ctx, manifest, r.RuntimeHome); err != nil {
		return "", err
	}
	runtime, err := VerifyRuntime(ctx, manifest, r.RuntimeHome, r.Identity.ConfigPath, args, agents, diskRuntimeFiles{})
	if err != nil {
		return "", err
	}
	configRaw, err := read(r.Identity.ConfigPath, 262144)
	config := string(configRaw)
	if err != nil || Digest(config) != first.ConfigSHA256 {
		return "", fmt.Errorf("participant configuration changed while inspecting runtime")
	}
	properties, err := simpleProperties(config)
	if err != nil {
		return "", err
	}
	inherited := map[string]string{}
	for key, value := range properties {
		lower := strings.ToLower(key)
		if strings.Contains(lower, "sasl") || strings.Contains(lower, "ssl") || strings.Contains(lower, "password") || strings.Contains(lower, "credential") || strings.Contains(lower, "token") || strings.Contains(lower, "secret") || strings.Contains(lower, "jaas") || strings.Contains(lower, "authorizer") || lower == "super.users" || lower == "security.protocol" {
			return "", fmt.Errorf("running security configuration requires an explicit expansion adapter")
		}
		if protectedProperties[key] || key == "cluster.id" {
			continue
		}
		inherited[key] = value
	}
	if value := properties["inter.broker.listener.name"]; value != "" && value != "PLAINTEXT" {
		return "", fmt.Errorf("custom inter-broker listener requires a supported adapter")
	}
	if value := properties["controller.listener.names"]; value != "CONTROLLER" {
		return "", fmt.Errorf("reviewed CONTROLLER listener required")
	}
	protocols := map[string]string{}
	for _, entry := range strings.Split(properties["listener.security.protocol.map"], ",") {
		kv := strings.SplitN(strings.TrimSpace(entry), ":", 2)
		if len(kv) != 2 || protocols[kv[0]] != "" {
			return "", fmt.Errorf("unambiguous listener protocol mapping required")
		}
		protocols[kv[0]] = kv[1]
	}
	if protocols["CONTROLLER"] != "PLAINTEXT" || protocols["PLAINTEXT"] != "PLAINTEXT" || len(protocols) != 2 {
		return "", fmt.Errorf("nonstandard listener security requires an expansion adapter")
	}
	controllerPort := 0
	if strings.Contains(r.Identity.Role, "controller") {
		for _, listener := range strings.Split(properties["listeners"], ",") {
			if !strings.HasPrefix(listener, "CONTROLLER://") {
				continue
			}
			value := strings.TrimPrefix(listener, "CONTROLLER://")
			split := strings.LastIndex(value, ":")
			if split < 1 {
				return "", fmt.Errorf("controller listener endpoint unavailable")
			}
			controllerPort, err = strconv.Atoi(value[split+1:])
			if err != nil || !port(controllerPort) {
				return "", fmt.Errorf("controller listener port unavailable")
			}
		}
		if controllerPort == 0 {
			return "", fmt.Errorf("controller listener was not observed")
		}
	}
	second, err := processinspect.RunWithReader(ctx, string(identityRaw), hostID, e, read)
	if err != nil || !second.IdentityVerified || first.PID != second.PID || first.ProcessStartTicks != second.ProcessStartTicks || first.ConfigSHA256 != second.ConfigSHA256 || first.UnitSHA256 != second.UnitSHA256 || !reflect.DeepEqual(first.Metadata, second.Metadata) {
		return "", fmt.Errorf("participant changed during runtime inspection")
	}
	report := RuntimeInspectionReport{SchemaVersion: r.SchemaVersion, Identity: second, Runtime: runtime, JavaAgents: agents, ResolvedPaths: bindings, Inherited: inherited, ControllerPort: controllerPort, ObservedAt: time.Now().UTC()}
	encoded, err := json.Marshal(report)
	return string(encoded), err
}
func resolveObservationPath(name, config, unitName string) (string, error) {
	allowed := name == config && safeConfig(name) || filepath.Base(name) == "meta.properties" && safePath(filepath.Dir(name)) || unitFilePath(name, unitName)
	if !allowed {
		return "", fmt.Errorf("file is outside the reviewed observation scope")
	}
	resolved, err := filepath.EvalSymlinks(name)
	if err != nil {
		return "", err
	}
	if filepath.Base(resolved) != filepath.Base(name) || !(safeConfig(resolved) || filepath.Base(resolved) == "meta.properties" && safePath(filepath.Dir(resolved)) || unitFilePath(resolved, unitName)) {
		return "", fmt.Errorf("resolved file is outside the supported adapter")
	}
	return resolved, nil
}
