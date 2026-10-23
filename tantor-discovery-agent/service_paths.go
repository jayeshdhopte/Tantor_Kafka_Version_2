package main

import (
	"context"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Only paths and endpoint ports are reported, never full environments/configuration contents.
type DataServicePath struct {
	ServiceType string `json:"serviceType"`
	ServiceName string `json:"serviceName"`
	RestPort    int    `json:"restPort"`
	InstallDir  string `json:"installDir"`
	ConfigFile  string `json:"configFile"`
	PluginDir   string `json:"pluginDir"`
	WorkingDir  string `json:"workingDir"`
	LogDir      string `json:"logDir"`
}

var dataServiceUnit = regexp.MustCompile(`^(schema-registry|confluent-schema-registry|kafka-connect|confluent-kafka-connect)([-@][A-Za-z0-9_.@-]+)?\.service$`)
var startExecutable = regexp.MustCompile(`(?:^|[ {;])path=([^ ;}]+)`)

func matchingServiceBootstrap(raw, bootstrap string) bool {
	normalize := func(value string) string {
		value = strings.TrimSpace(value)
		if index := strings.Index(value, "://"); index >= 0 {
			value = value[index+3:]
		}
		return strings.ToLower(value)
	}
	for _, a := range strings.Split(raw, ",") {
		for _, b := range strings.Split(bootstrap, ",") {
			if normalize(a) != "" && normalize(a) == normalize(b) {
				return true
			}
		}
	}
	return false
}

func dataServicePath(unit, start, cwd string, args, environment []string, props map[string]string, bootstrap string) (DataServicePath, bool) {
	result := DataServicePath{ServiceName: strings.TrimSuffix(unit, ".service"), WorkingDir: cwd}
	for i, arg := range args {
		if arg == "io.confluent.kafka.schemaregistry.rest.SchemaRegistryMain" {
			result.ServiceType = "SCHEMA_REGISTRY"
		}
		if arg == "org.apache.kafka.connect.cli.ConnectDistributed" {
			result.ServiceType = "KAFKA_CONNECT"
		}
		if result.ServiceType != "" && i+1 < len(args) {
			result.ConfigFile = args[i+1]
			break
		}
	}
	if result.ServiceType == "" || result.ConfigFile == "" || !dataServiceUnit.MatchString(unit) {
		return result, false
	}
	if !filepath.IsAbs(result.ConfigFile) {
		result.ConfigFile = filepath.Join(cwd, result.ConfigFile)
	}
	result.ConfigFile = filepath.Clean(result.ConfigFile)
	key := "bootstrap.servers"
	if result.ServiceType == "SCHEMA_REGISTRY" {
		key = "kafkastore.bootstrap.servers"
	}
	if !matchingServiceBootstrap(props[key], bootstrap) {
		return result, false
	}
	for _, listener := range strings.Split(props["listeners"], ",") {
		parsed, err := url.Parse(strings.TrimSpace(listener))
		if err != nil {
			continue
		}
		if port, err := strconv.Atoi(parsed.Port()); err == nil && port > 0 && port < 65536 {
			result.RestPort = port
			break
		}
	}
	if result.RestPort == 0 {
		if port, err := strconv.Atoi(props["rest.port"]); err == nil && port > 0 && port < 65536 {
			result.RestPort = port
		}
	}
	if result.RestPort == 0 {
		return result, false
	}
	if match := startExecutable.FindStringSubmatch(start); len(match) == 2 {
		executable := match[1]
		name := filepath.Base(executable)
		if filepath.IsAbs(executable) && (name == "schema-registry-start" || name == "connect-distributed.sh" || name == "connect-distributed") {
			result.InstallDir = filepath.Dir(filepath.Dir(executable))
		}
	}
	var plugins []string
	for _, path := range strings.Split(props["plugin.path"], ",") {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		if !filepath.IsAbs(path) {
			path = filepath.Join(cwd, path)
		}
		plugins = append(plugins, path)
	}
	result.PluginDir = strings.Join(plugins, ",")
	for _, entry := range environment {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if key == "LOG_DIR" || key == "KAFKA_LOG_DIR" || key == "SCHEMA_REGISTRY_LOG_DIR" {
			if filepath.IsAbs(value) {
				result.LogDir = value
			}
		}
	}
	return result, true
}

func discoverDataServicePaths(parent context.Context, bootstrap string) []DataServicePath {
	result := make([]DataServicePath, 0)
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	units, err := exec.CommandContext(ctx, "systemctl", "list-units", "--type=service", "--state=running", "--no-legend", "--plain").Output()
	if err != nil {
		return nil
	}
	for _, line := range strings.Split(string(units), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || !dataServiceUnit.MatchString(fields[0]) {
			continue
		}
		unit := fields[0]
		show, err := exec.CommandContext(ctx, "systemctl", "show", unit, "--property=MainPID", "--property=ExecStart").Output()
		if err != nil {
			continue
		}
		values := make(map[string]string)
		for _, property := range strings.Split(string(show), "\n") {
			k, v, ok := strings.Cut(property, "=")
			if ok {
				values[k] = v
			}
		}
		pid, err := strconv.Atoi(values["MainPID"])
		if err != nil || pid < 1 {
			continue
		}
		proc := filepath.Join("/proc", strconv.Itoa(pid))
		command, err := os.ReadFile(filepath.Join(proc, "cmdline"))
		if err != nil {
			continue
		}
		cwd, err := os.Readlink(filepath.Join(proc, "cwd"))
		if err != nil {
			continue
		}
		args := strings.Split(strings.TrimRight(string(command), "\x00"), "\x00")
		config := ""
		for i, arg := range args {
			if (arg == "io.confluent.kafka.schemaregistry.rest.SchemaRegistryMain" || arg == "org.apache.kafka.connect.cli.ConnectDistributed") && i+1 < len(args) {
				config = args[i+1]
				break
			}
		}
		if config == "" {
			continue
		}
		if !filepath.IsAbs(config) {
			config = filepath.Join(cwd, config)
		}
		content, err := os.ReadFile(config)
		if err != nil {
			continue
		}
		environment, _ := os.ReadFile(filepath.Join(proc, "environ"))
		path, ok := dataServicePath(unit, values["ExecStart"], cwd, args, strings.Split(string(environment), "\x00"), parseProperties(string(content)), bootstrap)
		if ok {
			result = append(result, path)
		}
	}
	if ctx.Err() != nil {
		return nil
	}
	return result
}
