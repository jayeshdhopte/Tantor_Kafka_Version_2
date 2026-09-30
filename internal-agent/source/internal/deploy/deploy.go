package deploy

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"io.translab/tantor-agent/internal/client"
	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/internal/deploy/connect"
	"io.translab/tantor-agent/internal/deploy/dataservice"
	"io.translab/tantor-agent/internal/deploy/kafka"
	"io.translab/tantor-agent/internal/deploy/ksqldb"
	"io.translab/tantor-agent/internal/deploy/monitoring"
	"io.translab/tantor-agent/internal/deploy/parcel"
	"io.translab/tantor-agent/internal/deploy/schema"
	"io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/internal/taskvalidate"
	"io.translab/tantor-agent/pkg/api"
)

// Engine handles the deployment of services
type Engine struct {
	cfg    *config.Config
	client *client.APIClient
	exec   executor.Executor
}

func NewEngine(cfg *config.Config, client *client.APIClient, exec executor.Executor) *Engine {
	return &Engine{
		cfg:    cfg,
		client: client,
		exec:   exec,
	}
}

// Execute handles a task dispatched from the task poller
func (e *Engine) Execute(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	slog.Info("Executing deployment task", "taskId", t.TaskID, "command", t.Command)

	switch t.Command {
	case "INSTALL_KAFKA":
		return e.installKafka(ctx, t)
	case "UPGRADE_KAFKA":
		return e.upgradeKafka(ctx, t)
	case "INSTALL_CONNECT":
		return e.installConnect(ctx, t)
	case "INSTALL_SCHEMA":
		return e.installSchema(ctx, t)
	case "SCHEMA_DIRECTORY_CANDIDATES", "CONNECT_DIRECTORY_CANDIDATES":
		return e.schemaDirectoryCandidates(ctx, t)
	case "PRECHECK_SCHEMA":
		return e.precheckSchema(ctx, t)
	case "PRECHECK_CONNECT":
		return e.precheckDataService(ctx, t, dataservice.Connect)
	case "VERIFY_SCHEMA_REGISTRY":
		return e.verifySchemaRegistry(ctx, t)
	case "VERIFY_CONNECT":
		return e.verifyDataService(ctx, t, dataservice.Connect, "connect_url")
	case "INSTALL_KSQLDB":
		return e.installKsql(ctx, t)
	case "INSTALL_MONITORING":
		return e.installMonitoring(ctx, t)
	case "DELETE_MONITORING":
		return e.deleteMonitoring(ctx, t)
	case "START_SERVICE":
		return e.startService(ctx, t)
	case "STOP_SERVICE":
		return e.stopService(ctx, t)
	case "RESTART_SERVICE":
		return e.restartService(ctx, t)
	case "UPDATE_KAFKA_CONFIG":
		return e.updateKafkaConfig(ctx, t)
	case "DELETE_CLUSTER":
		return e.deleteCluster(ctx, t)
	case "ROLLBACK_DEPLOYMENT":
		return e.rollbackDeployment(ctx, t)
	case "DISTRIBUTE_PARCEL":
		return e.distributeParcel(ctx, t)
	case "ACTIVATE_PARCEL":
		return e.activateParcel(ctx, t)
	case "DEACTIVATE_PARCEL":
		return e.deactivateParcel(ctx, t)
	case "REMOVE_PARCEL":
		return e.removeParcel(ctx, t)
	case "CHECK_PREREQUISITES":
		return e.checkPrerequisites(ctx, t)
	case "CHECK_PORTS":
		return e.checkPorts(ctx, t)
	case "APPLY_PREREQUISITES":
		return e.applyPrerequisites(ctx, t)
	case "REBOOT_HOST":
		return e.scheduleHostReboot(ctx, t)
	case "CHECK_KRAFT_CONNECTIVITY":
		return e.checkKRaftConnectivity(ctx, t)
	case "VERIFY_KRAFT_QUORUM":
		return e.verifyKRaftQuorum(ctx, t)
	case "VERIFY_ZK_QUORUM":
		return e.verifyZooKeeperQuorum(ctx, t)
	default:
		return &api.TaskResult{
			TaskID:   t.TaskID,
			HostID:   e.cfg.Agent.HostID,
			Status:   "FAILED",
			ErrorMsg: fmt.Sprintf("Unknown command: %s", t.Command),
		}, nil
	}
}

func (e *Engine) upgradeKafka(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	deployer := kafka.NewDeployer(e.cfg, e.client, e.exec)
	logOutput, err := deployer.Upgrade(ctx, t)
	if err != nil {
		return e.fail(t, fmt.Sprintf("Kafka upgrade failed: %v\nLogs: %s", err, logOutput)), nil
	}
	return &api.TaskResult{TaskID: t.TaskID, HostID: e.cfg.Agent.HostID, Status: "SUCCESS", LogOutput: logOutput}, nil
}

func (e *Engine) distributeParcel(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	deployer := parcel.NewDeployer(e.cfg, e.client, e.exec)
	logOutput, err := deployer.Distribute(ctx, t)
	if err != nil {
		return e.fail(t, fmt.Sprintf("Parcel distribution failed: %v\nLogs: %s", err, logOutput)), nil
	}
	return &api.TaskResult{TaskID: t.TaskID, HostID: e.cfg.Agent.HostID, Status: "SUCCESS", LogOutput: logOutput}, nil
}

func (e *Engine) activateParcel(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	deployer := parcel.NewDeployer(e.cfg, e.client, e.exec)
	logOutput, err := deployer.Activate(ctx, t)
	if err != nil {
		return e.fail(t, fmt.Sprintf("Parcel activation failed: %v\nLogs: %s", err, logOutput)), nil
	}
	return &api.TaskResult{TaskID: t.TaskID, HostID: e.cfg.Agent.HostID, Status: "SUCCESS", LogOutput: logOutput}, nil
}

func (e *Engine) deactivateParcel(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	deployer := parcel.NewDeployer(e.cfg, e.client, e.exec)
	logOutput, err := deployer.Deactivate(ctx, t)
	if err != nil {
		return e.fail(t, fmt.Sprintf("Parcel deactivation failed: %v\nLogs: %s", err, logOutput)), nil
	}
	return &api.TaskResult{TaskID: t.TaskID, HostID: e.cfg.Agent.HostID, Status: "SUCCESS", LogOutput: logOutput}, nil
}

func (e *Engine) removeParcel(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	deployer := parcel.NewDeployer(e.cfg, e.client, e.exec)
	logOutput, err := deployer.Remove(ctx, t)
	if err != nil {
		return e.fail(t, fmt.Sprintf("Parcel removal failed: %v\nLogs: %s", err, logOutput)), nil
	}
	return &api.TaskResult{TaskID: t.TaskID, HostID: e.cfg.Agent.HostID, Status: "SUCCESS", LogOutput: logOutput}, nil
}

func (e *Engine) deleteCluster(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	deployer := kafka.NewDeployer(e.cfg, e.client, e.exec)
	logOutput, err := deployer.Clean(ctx, t)
	if err != nil {
		return e.fail(t, fmt.Sprintf("Cluster cleanup failed: %v\nLogs: %s", err, logOutput)), nil
	}
	return &api.TaskResult{
		TaskID:    t.TaskID,
		HostID:    e.cfg.Agent.HostID,
		Status:    "SUCCESS",
		LogOutput: logOutput,
	}, nil
}

func (e *Engine) rollbackDeployment(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	deployer := kafka.NewDeployer(e.cfg, e.client, e.exec)
	logOutput, err := deployer.Rollback(ctx, t)
	if err != nil {
		return e.fail(t, fmt.Sprintf("Rollback failed: %v\nLogs: %s", err, logOutput)), nil
	}
	return &api.TaskResult{
		TaskID:    t.TaskID,
		HostID:    e.cfg.Agent.HostID,
		Status:    "SUCCESS",
		LogOutput: logOutput,
	}, nil
}

func (e *Engine) installKafka(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	deployer := kafka.NewDeployer(e.cfg, e.client, e.exec)
	currentStep := "Starting deployment"

	reporter := func(step string, logs string) {
		currentStep = step
		e.client.ReportTaskResult(&api.TaskResult{
			TaskID:      t.TaskID,
			ClaimToken:  t.ClaimToken,
			HostID:      e.cfg.Agent.HostID,
			Status:      "IN_PROGRESS",
			CurrentStep: step,
			LogOutput:   logs,
		})
	}

	logOutput, err := deployer.Deploy(ctx, t, reporter)

	if err != nil {
		failResult := e.fail(t, fmt.Sprintf("Kafka deployment failed: %v\nLogs: %s", err, logOutput))
		failResult.CurrentStep = currentStep
		failResult.FailedReason = friendlyDeploymentFailure(currentStep, err)
		failResult.LogOutput = logOutput
		return failResult, nil
	}

	return &api.TaskResult{
		TaskID:    t.TaskID,
		HostID:    e.cfg.Agent.HostID,
		Status:    "SUCCESS",
		LogOutput: logOutput,
	}, nil
}

func friendlyDeploymentFailure(step string, err error) string {
	detail := strings.ToLower(err.Error())
	switch {
	case strings.Contains(detail, "artifact reference is missing"), strings.Contains(detail, "neither artifact_url nor artifact_id"):
		return "The Kafka deployment task did not contain the uploaded artifact reference. Verify that the backend passes artifact_id or artifact_url from the UI upload record to the agent task."
	case strings.Contains(detail, "404"), strings.Contains(detail, "download"):
		return "Kafka could not be downloaded from the management server. Verify that the selected UI artifact still exists and that its download endpoint is reachable from the target VM."
	case strings.Contains(detail, "checksum"):
		return "The downloaded Kafka package failed integrity verification. Upload or distribute the binary again before retrying."
	case strings.Contains(detail, "permission denied"), strings.Contains(detail, "not permitted"):
		return "The agent does not have permission to write files or manage the Kafka service. Verify its sudo and directory permissions."
	case strings.Contains(detail, "no space left"), strings.Contains(detail, "disk"):
		return "The target host does not have enough usable disk space for Kafka. Free space and retry the deployment."
	case strings.Contains(detail, "format"), strings.Contains(detail, "storage"):
		return "Kafka storage initialization failed. Verify the Kafka cluster ID, node ID, and data-directory permissions before retrying."
	case strings.Contains(detail, "systemctl"), strings.Contains(detail, "service"):
		return "Kafka was installed but its service could not start. Check the service configuration and the technical logs below."
	case strings.Contains(detail, "port"), strings.Contains(detail, "listening"):
		return "Kafka did not become reachable on its configured port. Check port conflicts, listeners, and firewall settings."
	case strings.Contains(detail, "config"), strings.Contains(detail, "properties"):
		return "Kafka configuration generation or validation failed. Review the selected properties and node topology."
	default:
		return fmt.Sprintf("Kafka deployment failed during '%s'. Review the recommended checks and technical logs below.", step)
	}
}

func (e *Engine) startService(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	serviceName, err := validatedServiceName(t.Parameters["service_name"])
	if err != nil {
		return e.fail(t, err.Error()), nil
	}
	out, errOut, err := e.exec.RunSudo(ctx, "systemctl", "start", serviceName)
	if err != nil {
		return e.fail(t, fmt.Sprintf("Failed to start service: %v, out: %s, errOut: %s", err, out, errOut)), nil
	}

	return &api.TaskResult{
		TaskID:    t.TaskID,
		HostID:    e.cfg.Agent.HostID,
		Status:    "SUCCESS",
		LogOutput: fmt.Sprintf("Service %s started successfully.", serviceName),
	}, nil
}

func (e *Engine) stopService(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	serviceName, err := validatedServiceName(t.Parameters["service_name"])
	if err != nil {
		return e.fail(t, err.Error()), nil
	}
	out, errOut, err := e.exec.RunSudo(ctx, "systemctl", "stop", serviceName)
	if err != nil {
		return e.fail(t, fmt.Sprintf("Failed to stop service: %v, out: %s, errOut: %s", err, out, errOut)), nil
	}

	return &api.TaskResult{
		TaskID:    t.TaskID,
		HostID:    e.cfg.Agent.HostID,
		Status:    "SUCCESS",
		LogOutput: fmt.Sprintf("Service %s stopped successfully.", serviceName),
	}, nil
}

func (e *Engine) restartService(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	serviceName, err := validatedServiceName(t.Parameters["service_name"])
	if err != nil {
		return e.fail(t, err.Error()), nil
	}
	out, errOut, err := e.exec.RunSudo(ctx, "systemctl", "restart", serviceName)
	if err != nil {
		return e.fail(t, fmt.Sprintf("Failed to restart service: %v, out: %s, errOut: %s", err, out, errOut)), nil
	}

	return &api.TaskResult{
		TaskID:    t.TaskID,
		HostID:    e.cfg.Agent.HostID,
		Status:    "SUCCESS",
		LogOutput: fmt.Sprintf("Service %s restarted successfully.", serviceName),
	}, nil
}

func validatedServiceName(raw string) (string, error) {
	name := strings.TrimSuffix(strings.TrimSpace(raw), ".service")
	if kafkaName, err := taskvalidate.KafkaServiceName(name, "service_name"); err == nil {
		return kafkaName, nil
	}
	return taskvalidate.Identifier(name, "service_name",
		"kafka", "broker", "controller", "zookeeper",
		"kafka-connect", "schema-registry", "ksqldb", "prometheus", "grafana",
	)
}

func (e *Engine) updateKafkaConfig(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	deployer := kafka.NewDeployer(e.cfg, e.client, e.exec)
	logOutput, err := deployer.UpdateConfig(ctx, t)
	if err != nil {
		return e.fail(t, fmt.Sprintf("Kafka config update failed: %v\nLogs: %s", err, logOutput)), nil
	}
	return &api.TaskResult{
		TaskID:    t.TaskID,
		HostID:    e.cfg.Agent.HostID,
		Status:    "SUCCESS",
		LogOutput: logOutput,
	}, nil
}

func (e *Engine) checkPrerequisites(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	type prerequisiteCheck struct {
		name        string
		requirement string
		script      string
	}

	checks := []prerequisiteCheck{
		{
			name:        "Open file limit (soft/hard)",
			requirement: "both soft and hard limits must be at least 1024000",
			script: `
soft=$(ulimit -Sn 2>/dev/null || true)
hard=$(ulimit -Hn 2>/dev/null || true)
printf '%s/%s\n' "${soft:-unavailable}" "${hard:-unavailable}"
is_sufficient_limit() {
  value="$1"
  [ "$value" = "unlimited" ] && return 0
  case "$value" in
    ''|*[!0-9]*) return 1 ;;
  esac
  [ "$value" -ge 1024000 ]
}
is_sufficient_limit "$soft" && is_sufficient_limit "$hard"`,
		},
		{
			name:        "Swappiness",
			requirement: "must be 0",
			script: `
value=$(cat /proc/sys/vm/swappiness 2>/dev/null || true)
printf '%s\n' "${value:-unavailable}"
[ "$value" = "0" ]`,
		},
		{
			name:        "Transparent Huge Pages",
			requirement: "enabled policy must be never",
			script: `
value=$(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || true)
printf '%s\n' "${value:-unavailable}"
printf '%s\n' "$value" | grep -q '\[never\]'`,
		},
		{
			name:        "SELinux",
			requirement: "must be Disabled or Permissive",
			script: `
if ! command -v getenforce >/dev/null 2>&1; then
  echo "getenforce not found"
  exit 1
fi
value=$(getenforce 2>/dev/null || true)
printf '%s\n' "${value:-unavailable}"
[ "$value" = "Disabled" ] || [ "$value" = "Permissive" ]`,
		},
		{
			name:        "Java Version",
			requirement: "must be 17.x",
		},
		{
			name:        "NTP Service",
			requirement: "ntpd or chronyd must be active",
			script: `
if systemctl is-active --quiet ntpd 2>/dev/null; then
  echo "ntpd: Active"
  exit 0
fi
if systemctl is-active --quiet chronyd 2>/dev/null; then
  echo "chronyd: Active"
  exit 0
fi
echo "Not running"
exit 1`,
		},
	}

	var logs strings.Builder
	failed := 0
	passed := 0

	logs.WriteString("\n===== Kafka System Pre-check =====\n")
	for _, check := range checks {
		var out, errOut string
		var err error
		if check.name == "Java Version" {
			out, errOut, err = e.validateJava(ctx, t)
		} else {
			out, errOut, err = e.exec.Run(ctx, "bash", "-c", check.script)
		}
		detail := strings.TrimSpace(out)
		if detail == "" {
			detail = strings.TrimSpace(errOut)
		}
		if detail == "" {
			detail = "no value returned"
		}
		detail = strings.ReplaceAll(detail, "\n", "; ")

		if err != nil {
			failed++
			logs.WriteString(fmt.Sprintf("%s: %s [Fail, %s]\n", check.name, detail, check.requirement))
			continue
		}

		passed++
		logs.WriteString(fmt.Sprintf("%s: %s [Pass]\n", check.name, detail))
	}
	logs.WriteString("===== Pre-check Completed =====\n")
	logs.WriteString(fmt.Sprintf("Summary: %d passed, %d failed, %d total\n", passed, failed, len(checks)))

	if failed > 0 {
		message := fmt.Sprintf("Kafka prerequisite check failed: %d of %d required checks failed", failed, len(checks))
		return &api.TaskResult{
			TaskID:       t.TaskID,
			HostID:       e.cfg.Agent.HostID,
			Status:       "FAILED",
			LogOutput:    logs.String(),
			ErrorMsg:     message,
			FailedReason: message,
		}, nil
	}

	logs.WriteString("Kafka prerequisite check passed.\n")
	return &api.TaskResult{
		TaskID:    t.TaskID,
		HostID:    e.cfg.Agent.HostID,
		Status:    "SUCCESS",
		LogOutput: logs.String(),
	}, nil
}

func (e *Engine) checkPorts(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	ports, portErr := prerequisitePorts(t.Parameters["required_ports"])
	if portErr != nil {
		return e.fail(t, portErr.Error()), nil
	}
	var logs strings.Builder
	failed := 0

	logs.WriteString("\n===== Kafka Port Check =====\n")
	for _, port := range ports {
		listener, err := net.Listen("tcp", net.JoinHostPort("", port))
		detail := ""
		if err != nil {
			failed++
			logs.WriteString(fmt.Sprintf("Port %s: %s [Fail]\n", port, firstNonBlank(detail, "in use")))
			continue
		}
		_ = listener.Close()
		logs.WriteString(fmt.Sprintf("Port %s: %s [Pass]\n", port, firstNonBlank(detail, "available")))
	}
	logs.WriteString("===== Port Check Completed =====\n")

	if failed > 0 {
		message := fmt.Sprintf("Port check failed: %d of %d port(s) are not available", failed, len(ports))
		logs.WriteString(message + "\n")
		return &api.TaskResult{
			TaskID:       t.TaskID,
			HostID:       e.cfg.Agent.HostID,
			Status:       "FAILED",
			LogOutput:    logs.String(),
			ErrorMsg:     message,
			FailedReason: message,
		}, nil
	}

	logs.WriteString("Port check passed.\n")
	return &api.TaskResult{
		TaskID:    t.TaskID,
		HostID:    e.cfg.Agent.HostID,
		Status:    "SUCCESS",
		LogOutput: logs.String(),
	}, nil
}

func prerequisitePorts(raw string) ([]string, error) {
	seen := map[string]bool{}
	ports := make([]string, 0)
	if strings.TrimSpace(raw) == "" {
		return []string{"9092", "9093", "7071"}, nil
	}
	for _, value := range strings.Split(raw, ",") {
		port, err := taskvalidate.Port(value)
		if err != nil {
			return nil, fmt.Errorf("required_ports contains %q: %w", value, err)
		}
		if !seen[port] {
			seen[port] = true
			ports = append(ports, port)
		}
	}
	return ports, nil
}

func (e *Engine) validateJava(ctx context.Context, t *api.Task) (string, string, error) {
	javaHome := ""
	if t != nil && t.Parameters != nil {
		javaHome = firstNonBlank(t.Parameters["java_home"], t.Parameters["javaHome"])
	}
	candidates := []string{"java", "/usr/bin/java"}
	if javaHome != "" {
		cleanHome, err := taskvalidate.ApprovedPath(javaHome)
		if err != nil {
			return "", "", fmt.Errorf("invalid java_home: %w", err)
		}
		candidates = append([]string{filepath.Join(cleanHome, "bin", "java")}, candidates...)
	}
	for _, pattern := range []string{"/usr/lib/jvm/*/bin/java", "/usr/java/*/bin/java", "/opt/*/bin/java"} {
		matches, _ := filepath.Glob(pattern)
		candidates = append(candidates, matches...)
	}
	var lastErr error
	for _, candidate := range candidates {
		out, errOut, err := e.exec.Run(ctx, candidate, "-version")
		if err != nil {
			lastErr = err
			continue
		}
		versionOutput := strings.TrimSpace(firstNonBlank(errOut, out))
		version := javaVersion(versionOutput)
		if strings.HasPrefix(version, "17.") || version == "17" {
			return version, "", nil
		}
		return version, "", fmt.Errorf("Java 17.x is required")
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("Java not found")
	}
	return "", "Java not found. Provide java_home in the deployment request or set JAVA_HOME/PATH for the agent service.", lastErr
}

func javaVersion(output string) string {
	if start := strings.IndexByte(output, '"'); start >= 0 {
		if end := strings.IndexByte(output[start+1:], '"'); end >= 0 {
			return output[start+1 : start+1+end]
		}
	}
	fields := strings.Fields(output)
	for _, field := range fields {
		if strings.HasPrefix(field, "17.") || strings.HasPrefix(field, "1.") || (len(field) > 0 && field[0] >= '0' && field[0] <= '9') {
			return strings.Trim(field, `"'`)
		}
	}
	return output
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (e *Engine) checkKRaftConnectivity(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	endpoints, endpointErr := taskvalidate.Endpoints(t.Parameters["controller_endpoints"])
	if endpointErr != nil {
		return e.fail(t, fmt.Sprintf("invalid controller_endpoints: %v", endpointErr)), nil
	}
	if len(endpoints) == 0 {
		return e.fail(t, "controller_endpoints is required for KRaft connectivity validation"), nil
	}

	var logs strings.Builder
	for _, endpoint := range endpoints {
		connection, err := net.DialTimeout("tcp", endpoint, 5*time.Second)
		if err != nil {
			return e.fail(t, fmt.Sprintf("Controller endpoint %s is not reachable: %v\n%s", endpoint, err, logs.String())), nil
		}
		_ = connection.Close()
		logs.WriteString(fmt.Sprintf("[PASS] Controller endpoint %s is reachable\n", endpoint))
	}

	return &api.TaskResult{
		TaskID: t.TaskID, HostID: e.cfg.Agent.HostID, Status: "SUCCESS", LogOutput: logs.String(),
	}, nil
}

func (e *Engine) verifyKRaftQuorum(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	endpoints, endpointErr := taskvalidate.Endpoints(t.Parameters["controller_endpoints"])
	if endpointErr != nil {
		return e.fail(t, fmt.Sprintf("invalid controller_endpoints: %v", endpointErr)), nil
	}
	if len(endpoints) == 0 {
		return e.fail(t, "controller_endpoints is required for KRaft quorum verification"), nil
	}

	installBase := strings.TrimSpace(t.Parameters["kafka_install_dir"])
	if installBase == "" {
		installBase = "/opt"
	}
	var pathErr error
	installBase, pathErr = taskvalidate.ApprovedPath(installBase)
	if pathErr != nil {
		return e.fail(t, fmt.Sprintf("invalid kafka_install_dir: %v", pathErr)), nil
	}
	activeDir := installBase
	if filepath.Base(filepath.Clean(activeDir)) != "kafka" {
		activeDir = filepath.Join(activeDir, "kafka")
	}
	quorumScript := filepath.Join(activeDir, "bin", "kafka-metadata-quorum.sh")
	out, errOut, err := e.exec.Run(ctx, quorumScript, "--bootstrap-controller", endpoints[0], "describe", "--status")
	output := strings.TrimSpace(strings.TrimSpace(out) + "\n" + strings.TrimSpace(errOut))
	if err != nil {
		return e.fail(t, fmt.Sprintf("KRaft quorum status command failed: %v\n%s", err, output)), nil
	}

	expectedClusterID := strings.TrimSpace(t.Parameters["cluster_uuid"])
	if expectedClusterID == "" || !strings.Contains(output, expectedClusterID) {
		return e.fail(t, fmt.Sprintf("Quorum status did not report expected cluster ID %q\n%s", expectedClusterID, output)), nil
	}
	if !strings.Contains(output, "LeaderId:") {
		return e.fail(t, "Quorum status did not report a controller leader\n"+output), nil
	}
	if strings.Contains(output, "LeaderId: -1") {
		return e.fail(t, "KRaft quorum has no elected controller leader\n"+output), nil
	}
	expectedControllerCount, _ := strconv.Atoi(strings.TrimSpace(t.Parameters["expected_controller_count"]))
	if expectedControllerCount > 0 {
		currentVoterCount := 0
		for _, line := range strings.Split(output, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "CurrentVoters:") {
				currentVoterCount = strings.Count(line, "\"id\"")
				break
			}
		}
		if currentVoterCount != expectedControllerCount {
			return e.fail(t, fmt.Sprintf(
				"KRaft quorum reports %d current voters; expected %d\n%s",
				currentVoterCount, expectedControllerCount, output,
			)), nil
		}
	}

	return &api.TaskResult{
		TaskID: t.TaskID, HostID: e.cfg.Agent.HostID, Status: "SUCCESS",
		LogOutput: "[PASS] Runtime KRaft quorum is healthy\n" + output,
	}, nil
}

func (e *Engine) verifyZooKeeperQuorum(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	endpoints, endpointErr := taskvalidate.Endpoints(t.Parameters["zookeeper_connect"])
	if endpointErr != nil {
		return e.fail(t, fmt.Sprintf("invalid zookeeper_connect: %v", endpointErr)), nil
	}
	if len(endpoints) == 0 {
		return e.fail(t, "zookeeper_connect is required for ZooKeeper quorum verification"), nil
	}

	installBase := strings.TrimSpace(t.Parameters["kafka_install_dir"])
	if installBase == "" {
		installBase = "/opt"
	}
	var pathErr error
	installBase, pathErr = taskvalidate.ApprovedPath(installBase)
	if pathErr != nil {
		return e.fail(t, fmt.Sprintf("invalid kafka_install_dir: %v", pathErr)), nil
	}
	activeDir := installBase
	if filepath.Base(filepath.Clean(activeDir)) != "kafka" {
		activeDir = filepath.Join(activeDir, "kafka")
	}
	zkShellScript := filepath.Join(activeDir, "bin", "zookeeper-shell.sh")

	var logs strings.Builder
	for _, endpoint := range endpoints {
		out, errOut, err := e.exec.Run(ctx, zkShellScript, endpoint, "ls", "/")
		output := strings.TrimSpace(strings.TrimSpace(out) + "\n" + strings.TrimSpace(errOut))
		if err != nil {
			return e.fail(t, fmt.Sprintf("ZooKeeper quorum status command failed for endpoint %s: %v\n%s", endpoint, err, output)), nil
		}
		if !strings.Contains(output, "[") {
			return e.fail(t, fmt.Sprintf("ZooKeeper quorum status did not report a valid response for endpoint %s\n%s", endpoint, output)), nil
		}
		logs.WriteString(fmt.Sprintf("[PASS] ZooKeeper quorum is healthy via endpoint %s\n", endpoint))
	}

	return &api.TaskResult{
		TaskID: t.TaskID, HostID: e.cfg.Agent.HostID, Status: "SUCCESS",
		LogOutput: logs.String(),
	}, nil
}

func (e *Engine) fail(t *api.Task, msg string) *api.TaskResult {
	slog.Error("Task failed", "taskId", t.TaskID, "error", msg)
	return &api.TaskResult{
		TaskID:   t.TaskID,
		HostID:   e.cfg.Agent.HostID,
		Status:   "FAILED",
		ErrorMsg: msg,
	}
}

func (e *Engine) installConnect(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	deployer := connect.NewDeployer(e.cfg, e.client, e.exec)
	logOutput, err := deployer.Deploy(ctx, t)

	if err != nil {
		return e.fail(t, fmt.Sprintf("Connect deployment failed: %v\nLogs: %s", err, logOutput)), nil
	}

	return &api.TaskResult{
		TaskID:    t.TaskID,
		HostID:    e.cfg.Agent.HostID,
		Status:    "SUCCESS",
		LogOutput: logOutput,
	}, nil
}

func (e *Engine) installSchema(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	deployer := schema.NewDeployer(e.cfg, e.client, e.exec)
	logOutput, err := deployer.Deploy(ctx, t)
	if err != nil {
		return e.fail(t, fmt.Sprintf("Schema Registry deployment failed: %v\nLogs: %s", err, logOutput)), nil
	}
	return &api.TaskResult{
		TaskID:    t.TaskID,
		HostID:    e.cfg.Agent.HostID,
		Status:    "SUCCESS",
		LogOutput: logOutput,
	}, nil
}

func (e *Engine) installKsql(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	deployer := ksqldb.NewDeployer(e.cfg, e.client, e.exec)
	logOutput, err := deployer.Deploy(ctx, t)
	if err != nil {
		return e.fail(t, fmt.Sprintf("ksqlDB deployment failed: %v\nLogs: %s", err, logOutput)), nil
	}
	return &api.TaskResult{
		TaskID:    t.TaskID,
		HostID:    e.cfg.Agent.HostID,
		Status:    "SUCCESS",
		LogOutput: logOutput,
	}, nil
}

func (e *Engine) installMonitoring(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	deployer := monitoring.NewDeployer(e.cfg, e.client, e.exec)
	logOutput, err := deployer.Deploy(ctx, t)
	if err != nil {
		return e.fail(t, fmt.Sprintf("Monitoring deployment failed: %v\nLogs: %s", err, logOutput)), nil
	}
	return &api.TaskResult{
		TaskID:    t.TaskID,
		HostID:    e.cfg.Agent.HostID,
		Status:    "SUCCESS",
		LogOutput: logOutput,
	}, nil
}

func (e *Engine) deleteMonitoring(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	installDir := t.Parameters["install_dir"]
	if installDir == "" {
		return e.fail(t, "install_dir is required for monitoring deletion"), nil
	}
	if _, errOut, err := e.exec.RunSudo(ctx, "systemctl", "disable", "--now", "prometheus", "grafana"); err != nil {
		return e.fail(t, fmt.Sprintf("Failed to stop monitoring services: %v: %s", err, errOut)), nil
	}
	if _, errOut, err := e.exec.RunSudo(ctx, "rm", "-f", "/etc/systemd/system/prometheus.service", "/etc/systemd/system/grafana.service"); err != nil {
		return e.fail(t, fmt.Sprintf("Failed to remove monitoring unit files: %v: %s", err, errOut)), nil
	}
	if _, errOut, err := e.exec.RunSudo(ctx, "rm", "-rf", installDir); err != nil {
		return e.fail(t, fmt.Sprintf("Failed to remove monitoring directory: %v: %s", err, errOut)), nil
	}
	if _, errOut, err := e.exec.RunSudo(ctx, "systemctl", "daemon-reload"); err != nil {
		return e.fail(t, fmt.Sprintf("Failed to reload systemd: %v: %s", err, errOut)), nil
	}
	return &api.TaskResult{
		TaskID:    t.TaskID,
		HostID:    e.cfg.Agent.HostID,
		Status:    "SUCCESS",
		LogOutput: "Monitoring services stopped and files removed.",
	}, nil
}
