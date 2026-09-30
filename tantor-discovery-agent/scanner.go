package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// ProcessInfo describes one running Kafka server process or systemd service.
// Only broker/controller server processes are returned by the scanner. Kafka
// Connect, Schema Registry, exporters, and other Kafka-adjacent Java processes
// are intentionally excluded.
type ProcessInfo struct {
	Cmdline     string
	SystemdUnit string
	Cwd         string
	Pid         string
	PropsFile   string
	AppLogDir   string
}

func cleanCmdToken(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "\"'{}[](),;")
	return value
}

func extractPropsPath(cmdline, cwd string) string {
	// Kafka's actual server properties file is normally the last *.properties
	// argument. Iterate backwards so an unrelated JVM -D...properties argument
	// cannot win over broker.properties/controller.properties.
	fields := strings.Fields(strings.ReplaceAll(cmdline, "\\x00", " "))
	for i := len(fields) - 1; i >= 0; i-- {
		f := cleanCmdToken(fields[i])
		if !strings.HasSuffix(strings.ToLower(f), ".properties") {
			continue
		}
		// Ignore -Dkey=/path/file.properties style JVM settings. The Kafka server
		// configuration is a standalone positional argument.
		if strings.HasPrefix(f, "-D") || strings.Contains(f, "=") {
			continue
		}
		if !filepath.IsAbs(f) && cwd != "" {
			candidate := filepath.Clean(filepath.Join(cwd, f))
			if abs, err := filepath.Abs(candidate); err == nil {
				return abs
			}
		}
		if abs, err := filepath.Abs(f); err == nil {
			return abs
		}
		return filepath.Clean(f)
	}
	return ""
}

func extractKafkaAppLogDir(cmdline string) string {
	re := regexp.MustCompile(`(?:^|\s)-Dkafka\.logs\.dir=([^\s]+)`)
	m := re.FindStringSubmatch(cmdline)
	if len(m) != 2 {
		return ""
	}
	return strings.Trim(cleanCmdToken(m[1]), "\"")
}

func systemdKafkaAppLogDir(unit, execStart string) string {
	if dir := extractKafkaAppLogDir(execStart); dir != "" {
		return dir
	}

	unit = strings.TrimSpace(unit)
	if unit == "" {
		return ""
	}

	checks := [][]string{
		{"show", unit, "--property=Environment", "--value"},
		{"cat", unit, "--no-pager"},
	}
	for _, args := range checks {
		out, err := exec.Command("systemctl", args...).Output()
		if err != nil {
			continue
		}
		if dir := extractKafkaAppLogDir(string(out)); dir != "" {
			return dir
		}
	}
	return ""
}

func isKafkaServerProcess(cmdline string) bool {
	lower := strings.ToLower(cmdline)
	// Explicitly exclude common non-server Kafka JVMs. They often contain Kafka
	// jars and a *.properties argument and previously polluted server discovery.
	excluded := []string{
		"connectdistributed",
		"connectstandalone",
		"schemaregistrymain",
		"kafka.tools.",
		"kafka-mirror-maker",
		"mirrormaker",
		"kafkaexporter",
		"jmx_prometheus",
	}
	for _, marker := range excluded {
		if strings.Contains(lower, marker) {
			return false
		}
	}

	return strings.Contains(cmdline, "kafka.Kafka") ||
		strings.Contains(lower, "kafka-server-start") ||
		strings.Contains(lower, "kafka-server-start.sh")
}

func processKey(p ProcessInfo) string {
	if p.Pid != "" {
		return "pid:" + p.Pid
	}
	return "unit:" + p.SystemdUnit + "|cfg:" + p.PropsFile
}

func getRunningKafkaPropsFiles() []ProcessInfo {
	var result []ProcessInfo
	seen := map[string]bool{}

	// 1. Linux /proc based detection. This is the most accurate source because it
	// gives us the exact config file actually used by the running JVM.
	out, _ := exec.Command("pgrep", "java").Output()
	for _, pid := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		pid = strings.TrimSpace(pid)
		if pid == "" {
			continue
		}
		cmdline := readProcessCmdline(pid)
		if cmdline == "" || !isKafkaServerProcess(cmdline) {
			continue
		}
		cwd := readProcessCwd(pid)
		props := extractPropsPath(cmdline, cwd)
		if props == "" {
			continue
		}
		info := ProcessInfo{
			Cmdline:   cmdline,
			Cwd:       cwd,
			Pid:       pid,
			PropsFile: props,
			AppLogDir: extractKafkaAppLogDir(cmdline),
		}
		key := processKey(info)
		if !seen[key] {
			seen[key] = true
			result = append(result, info)
		}
	}

	// 2. systemd fallback. Useful when /proc is mounted with hidepid and the
	// limited agent user cannot inspect Kafka JVM command lines.
	for _, info := range getSystemdRunningKafkaPropsFiles() {
		key := processKey(info)
		if !seen[key] {
			seen[key] = true
			result = append(result, info)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].PropsFile < result[j].PropsFile
	})
	return result
}

func readProcessCwd(pid string) string {
	cwd, err := os.Readlink(fmt.Sprintf("/proc/%s/cwd", pid))
	if err == nil {
		return cwd
	}
	return ""
}

func readProcessCmdline(pid string) string {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%s/cmdline", pid))
	if err == nil {
		return strings.TrimSpace(strings.ReplaceAll(string(data), "\x00", " "))
	}
	// ps is only a fallback. On hardened systems it may expose less detail than
	// /proc, but still gives us a chance to discover a running Kafka server.
	out, err := exec.Command("ps", "-p", pid, "-o", "args=").Output()
	if err == nil {
		return strings.TrimSpace(string(out))
	}
	return ""
}

func getSystemdRunningKafkaPropsFiles() []ProcessInfo {
	var result []ProcessInfo
	if err := exec.Command("systemctl", "--version").Run(); err != nil {
		return result
	}

	out, _ := exec.Command("systemctl", "list-units", "--type=service", "--state=running", "--no-legend", "--plain").Output()
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		unit := fields[0]
		execStartOut, _ := exec.Command("systemctl", "show", unit, "--property=ExecStart", "--value").Output()
		execStart := strings.TrimSpace(string(execStartOut))
		if !isKafkaServerProcess(execStart) {
			continue
		}
		cwdOut, _ := exec.Command("systemctl", "show", unit, "--property=WorkingDirectory", "--value").Output()
		cwd := strings.TrimSpace(string(cwdOut))
		props := extractPropsPath(execStart, cwd)
		if props == "" {
			continue
		}
		result = append(result, ProcessInfo{
			Cmdline:     execStart,
			SystemdUnit: unit,
			Cwd:         cwd,
			PropsFile:   props,
			AppLogDir:   systemdKafkaAppLogDir(unit, execStart),
		})
	}
	return result
}

// =========================================================================
// Filesystem scanning
// =========================================================================

func findAllConfigProperties(scanPaths []string) []string {
	var results []string
	seen := map[string]bool{}

	for _, base := range scanPaths {
		info, err := os.Stat(base)
		if err != nil || !info.IsDir() {
			continue
		}

		_ = filepath.Walk(base, func(path string, fi os.FileInfo, err error) error {
			if err != nil || fi == nil {
				return nil
			}
			rel, _ := filepath.Rel(base, path)
			if strings.Count(rel, string(os.PathSeparator)) > 8 {
				if fi.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if fi.IsDir() {
				name := fi.Name()
				if name == ".git" || name == "node_modules" || name == "__pycache__" {
					return filepath.SkipDir
				}
				if base == "/" && (path == "/proc" || path == "/sys" || path == "/dev" || path == "/run" || path == "/boot" || path == "/tmp" || path == "/etc" || path == "/lib" || path == "/lib64" || path == "/usr/lib") {
					return filepath.SkipDir
				}
				return nil
			}

			name := strings.ToLower(fi.Name())
			if name != "server.properties" && name != "broker.properties" && name != "controller.properties" {
				return nil
			}
			abs, _ := filepath.Abs(path)
			if !seen[abs] {
				seen[abs] = true
				results = append(results, abs)
			}
			return nil
		})
	}
	sort.Strings(results)
	return results
}
