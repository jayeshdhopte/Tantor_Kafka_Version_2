package deploy

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"io.translab/tantor-agent/pkg/api"
)

func (e *Engine) applyPrerequisites(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	var logs strings.Builder
	failed := 0
	rebootRequired := false
	apply := func(name, command string) {
		out, errOut, err := e.exec.RunSudo(ctx, "bash", "-c", command)
		detail := strings.TrimSpace(strings.TrimSpace(out) + " " + strings.TrimSpace(errOut))
		if err != nil {
			failed++
			logs.WriteString(fmt.Sprintf("[FAIL] %s - %v: %s\n", name, err, detail))
			return
		}
		if detail == "" {
			detail = "applied"
		}
		logs.WriteString(fmt.Sprintf("[PASS] %s - %s\n", name, detail))
	}

	logs.WriteString("Applying Tantor Kafka prerequisites\n")
	logs.WriteString("=====================================\n")
	apply("Open File Limit", `
install -d -m 0755 /etc/security/limits.d /etc/systemd/system.conf.d /etc/systemd/user.conf.d
printf '%s\n' '* soft nofile 1024000' '* hard nofile 1024000' '* soft nproc 1024000' '* hard nproc 1024000' > /etc/security/limits.d/99-tantor-kafka.conf
printf '%s\n' '[Manager]' 'DefaultLimitNOFILE=1024000' > /etc/systemd/system.conf.d/99-tantor-kafka.conf
printf '%s\n' '[Manager]' 'DefaultLimitNOFILE=1024000' > /etc/systemd/user.conf.d/99-tantor-kafka.conf
echo 'persistent limit=1024000'`)
	currentLimit, limitErr := strconv.Atoi(strings.TrimSpace(func() string {
		out, _, _ := e.exec.Run(ctx, "bash", "-c", "ulimit -n")
		return out
	}()))
	if limitErr != nil || currentLimit < 1024000 {
		rebootRequired = true
		logs.WriteString("[WARN] Open File Limit - persistent value applied; agent restart or reboot is required\n")
	}

	apply("Swappiness", `install -d -m 0755 /etc/sysctl.d && printf '%s\n' 'vm.swappiness = 0' > /etc/sysctl.d/99-tantor-kafka.conf && sysctl -w vm.swappiness=0`)
	apply("Transparent Huge Pages", `
echo never > /sys/kernel/mm/transparent_hugepage/enabled
echo never > /sys/kernel/mm/transparent_hugepage/defrag
printf '%s\n' '[Unit]' 'Description=Disable Transparent Huge Pages for Kafka' 'After=local-fs.target' '' '[Service]' 'Type=oneshot' 'ExecStart=/bin/sh -c "echo never > /sys/kernel/mm/transparent_hugepage/enabled; echo never > /sys/kernel/mm/transparent_hugepage/defrag"' 'RemainAfterExit=yes' '' '[Install]' 'WantedBy=multi-user.target' > /etc/systemd/system/tantor-disable-thp.service
systemctl daemon-reload
systemctl enable --now tantor-disable-thp.service >/dev/null
echo 'THP=never'`)

	apply("SELinux", `
if [ -f /etc/selinux/config ]; then sed -ri 's/^SELINUX=.*/SELINUX=disabled/' /etc/selinux/config; fi
if command -v setenforce >/dev/null 2>&1; then setenforce 0 || true; fi
echo "SELinux configured disabled; current=$(command -v getenforce >/dev/null 2>&1 && getenforce || echo Disabled)"`)
	if out, _, _ := e.exec.Run(ctx, "bash", "-c", "command -v getenforce >/dev/null 2>&1 && getenforce || echo Disabled"); !strings.EqualFold(strings.TrimSpace(out), "Disabled") {
		rebootRequired = true
		logs.WriteString("[WARN] SELinux - reboot is required to transition from Permissive to Disabled\n")
	}

	apply("Time Synchronization", `
if systemctl is-active --quiet chronyd || systemctl is-active --quiet ntpd; then echo 'chronyd/ntpd already active'; exit 0; fi
if command -v chronyd >/dev/null 2>&1; then
  systemctl enable --now chronyd >/dev/null
  systemctl is-active --quiet chronyd
  echo 'chronyd active'
  exit 0
fi
if command -v ntpd >/dev/null 2>&1; then
  systemctl enable --now ntpd >/dev/null
  systemctl is-active --quiet ntpd
  echo 'ntpd active'
  exit 0
fi
echo 'No supported time-sync daemon is installed. Air-gapped mode never installs packages automatically; pre-stage chrony/ntp from approved offline media or an internal repository, then retry.' >&2
exit 1`)

	out, errOut, err := e.validateJava(ctx, t)
	if err != nil {
		failed++
		logs.WriteString(fmt.Sprintf("[FAIL] Java Version - Java 17.x is required for Kafka; no automatic Java switch was performed (%s %s)\n", out, errOut))
	} else {
		logs.WriteString(fmt.Sprintf("[PASS] Java Version - %s\n", strings.TrimSpace(out)))
	}

	logs.WriteString("=====================================\n")
	if failed > 0 {
		message := fmt.Sprintf("Prerequisite remediation failed: %d setting(s) could not be applied", failed)
		return &api.TaskResult{TaskID: t.TaskID, HostID: e.cfg.Agent.HostID, Status: "FAILED", LogOutput: logs.String(), ErrorMsg: message, FailedReason: message}, nil
	}
	if rebootRequired {
		logs.WriteString("All persistent settings were applied. Reboot the host, wait for the agent to reconnect, and run Check prerequisites again.\n")
		return &api.TaskResult{TaskID: t.TaskID, HostID: e.cfg.Agent.HostID, Status: "REBOOT_REQUIRED", LogOutput: logs.String()}, nil
	}
	logs.WriteString("Prerequisites fixed successfully. Run Check prerequisites again before deployment.\n")
	return &api.TaskResult{TaskID: t.TaskID, HostID: e.cfg.Agent.HostID, Status: "SUCCESS", LogOutput: logs.String()}, nil
}

func (e *Engine) scheduleHostReboot(ctx context.Context, t *api.Task) (*api.TaskResult, error) {
	out, errOut, err := e.exec.RunSudo(ctx, "systemd-run", "--unit=tantor-prerequisite-reboot", "--on-active=10s", "/usr/bin/systemctl", "reboot")
	if err != nil {
		return e.fail(t, fmt.Sprintf("Failed to schedule host reboot: %v (%s %s)", err, out, errOut)), nil
	}
	return &api.TaskResult{
		TaskID: t.TaskID, HostID: e.cfg.Agent.HostID, Status: "SUCCESS",
		LogOutput: "Host reboot scheduled in 10 seconds. The agent should reconnect automatically after boot.",
	}, nil
}
