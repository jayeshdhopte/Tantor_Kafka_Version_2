// Package dataservice implements only Schema Registry and Kafka Connect tasks.
// It does not manage Kafka services, storage, configuration or prerequisites.
package dataservice

import (
	"encoding/json"
	"fmt"
	"net"
	"path"
	"regexp"
	"strconv"
	"strings"

	"io.translab/tantor-agent/pkg/api"
)

const Schema = "schema-registry"
const Connect = "kafka-connect"

type Settings struct {
	Kind, Install, Config, Log, Working, Plugin, HeapMin, HeapMax, User, Group, Host string
	Port, Replication, MinDisk                                                       int
	Bootstrap, KafkaBin, Compatibility, GroupID, SchemaTopic                         string
	OffsetTopic, ConfigTopic, StatusTopic, HAPeers                                   string
	Deferred, SharedConnect, SharedSchema                                            bool
	Protected                                                                        []string
}

var heapPattern = regexp.MustCompile(`^[1-9][0-9]*[mMgG]$`)
var identityPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_-]*\$?$`)
var namePattern = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)
var pathPattern = regexp.MustCompile(`^/[a-zA-Z0-9_./+-]+$`)
var hostPattern = regexp.MustCompile(`^[a-zA-Z0-9.:-]+$`)

func Value(p map[string]string, key, fallback string) string {
	if value := strings.TrimSpace(p[key]); value != "" {
		return value
	}
	return fallback
}

func SafePath(value string) error {
	if !pathPattern.MatchString(value) || path.Clean(value) != value || value == "/" {
		return fmt.Errorf("path must be a clean absolute non-root path without spaces or traversal: %q", value)
	}
	return nil
}

func Overlaps(a, b string) bool {
	a, b = path.Clean(a), path.Clean(b)
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

func Parse(kind string, task *api.Task) (Settings, error) {
	s := Settings{Kind: kind}
	if kind != Schema && kind != Connect {
		return s, fmt.Errorf("unsupported data service %q", kind)
	}
	p := task.Parameters
	kafkaInstall := Value(p, "kafka_install_dir", "/opt/kafka")
	defaultInstall := kafkaInstall
	s.Install = Value(p, "install_dir", defaultInstall)
	s.SharedConnect = kind == Connect && s.Install == kafkaInstall
	s.SharedSchema = kind == Schema && s.Install == kafkaInstall
	defaultConfig := s.Install + "/config"
	port := "8083"
	if kind == Schema {
		defaultConfig = s.Install + "/etc/schema-registry"
		port = "8081"
	}
	s.Config = Value(p, "config_dir", defaultConfig)
	s.Log = Value(p, "log_dir", "/var/log/tantor/"+kind)
	s.Working = Value(p, "working_dir", "/var/lib/tantor/"+kind)
	defaultPlugin := s.Install + "/plugins"
	if s.SharedConnect {
		defaultPlugin = s.Install + "/connect-plugins"
	}
	s.Plugin = Value(p, "plugin_dir", defaultPlugin)
	heap := Value(p, "heap_size", "1G")
	s.HeapMin = Value(p, "heap_xms", heap)
	s.HeapMax = Value(p, "heap_xmx", heap)
	s.User = Value(p, "service_user", "root")
	s.Group = Value(p, "service_group", s.User)
	s.Host = Value(p, "host_name", Value(p, "host_ip", "localhost"))
	s.GroupID = Value(p, "group_id", "tantor-"+kind)
	s.SchemaTopic = Value(p, "schemas_topic", Value(p, "kafkastore_topic", "_schemas"))
	s.OffsetTopic = Value(p, "offset_topic", "connect-offsets")
	s.ConfigTopic = Value(p, "config_topic", "connect-configs")
	s.StatusTopic = Value(p, "status_topic", "connect-status")
	s.Compatibility = strings.ToUpper(Value(p, "compatibility_level", "BACKWARD"))
	s.Deferred = strings.EqualFold(p["allow_deferred_kafka"], "true")
	s.HAPeers = Value(p, "sr_ha_peers", "")
	var err error
	if s.Port, err = strconv.Atoi(Value(p, "rest_port", port)); err != nil || s.Port < 1 || s.Port > 65535 {
		return s, fmt.Errorf("invalid REST port")
	}
	if s.Replication, err = strconv.Atoi(Value(p, "replication_factor", "1")); err != nil || s.Replication < 1 || s.Replication > 32767 {
		return s, fmt.Errorf("invalid replication factor")
	}
	if s.MinDisk, err = strconv.Atoi(Value(p, "min_free_disk_mb", "5120")); err != nil || s.MinDisk < 5120 {
		return s, fmt.Errorf("Connect working disk requirement must be at least 5120 MiB")
	}
	minMiB, minErr := heapMiB(s.HeapMin)
	maxMiB, maxErr := heapMiB(s.HeapMax)
	if minErr != nil || maxErr != nil {
		return s, fmt.Errorf("heap_xms and heap_xmx must be positive sizes such as 512M or 1G")
	}
	if minMiB > maxMiB {
		return s, fmt.Errorf("heap_xms cannot exceed heap_xmx")
	}
	if !identityPattern.MatchString(s.User) || !identityPattern.MatchString(s.Group) {
		return s, fmt.Errorf("invalid service user/group")
	}
	if !hostPattern.MatchString(s.Host) {
		return s, fmt.Errorf("invalid advertised service hostname")
	}
	for _, name := range []string{s.GroupID, s.SchemaTopic, s.OffsetTopic, s.ConfigTopic, s.StatusTopic} {
		if !namePattern.MatchString(name) || name == "." || name == ".." {
			return s, fmt.Errorf("invalid group/topic name %q", name)
		}
	}
	switch s.Compatibility {
	case "BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE", "FULL", "FULL_TRANSITIVE", "NONE":
	default:
		return s, fmt.Errorf("invalid compatibility level")
	}
	s.Bootstrap, err = Endpoints(p["bootstrap_servers"])
	if err != nil {
		return s, err
	}
	if s.HAPeers != "" {
		if s.HAPeers, err = Endpoints(s.HAPeers); err != nil {
			return s, err
		}
	}
	if err = SafePath(kafkaInstall); err != nil {
		return s, err
	}
	s.KafkaBin = kafkaInstall + "/bin"
	s.Protected = []string{kafkaInstall, Value(p, "kafka_data_dir", "/data/kafka"), Value(p, "kafka_log_dir", "/var/log/kafka"), "/etc/kafka", "/var/lib/kafka"}
	other := Connect
	if kind == Connect {
		other = Schema
	}
	s.Protected = append(s.Protected, "/opt/tantor/"+other, "/var/log/tantor/"+other, "/var/lib/tantor/"+other)
	// External discovery supplies every locally observed Kafka tree, including
	// metadata directories and custom paths belonging to other local clusters.
	if raw := p["protected_paths"]; raw != "" {
		var extra []string
		if err := json.Unmarshal([]byte(raw), &extra); err != nil {
			return s, fmt.Errorf("invalid protected paths: %w", err)
		}
		s.Protected = append(s.Protected, extra...)
	}
	for _, key := range []string{"other_install_dir", "other_config_dir", "other_log_dir", "other_working_dir", "other_plugin_dir"} {
		if p[key] != "" {
			s.Protected = append(s.Protected, p[key])
		}
	}
	for _, protected := range s.Protected {
		if err = SafePath(protected); err != nil {
			return s, err
		}
	}
	for _, dir := range s.Directories() {
		if err = SafePath(dir); err != nil {
			return s, err
		}
		for _, protected := range s.Protected {
			if Overlaps(dir, protected) && !s.allowedSharedPath(dir, protected, kafkaInstall) {
				return s, fmt.Errorf("data-service path %s overlaps protected path %s", dir, protected)
			}
		}
	}
	if s.SharedConnect {
		if Overlaps(s.Config, s.Install) && s.Config != s.Install+"/config" {
			return s, fmt.Errorf("shared Connect config must use Kafka's config directory")
		}
		if Overlaps(s.Plugin, s.Install) && s.Plugin != s.Install+"/connect-plugins" {
			return s, fmt.Errorf("shared Connect plugins must use Kafka's connect-plugins directory")
		}
		for _, dir := range []string{s.Log, s.Working} {
			if Overlaps(dir, s.Install) {
				return s, fmt.Errorf("Connect log/work paths must be outside the shared Kafka installation")
			}
		}
	}
	if s.SharedSchema {
		if s.Config != s.Install+"/etc/schema-registry" {
			return s, fmt.Errorf("shared Schema Registry config must use Kafka's etc/schema-registry directory")
		}
		for _, dir := range []string{s.Log, s.Working} {
			if Overlaps(dir, s.Install) {
				return s, fmt.Errorf("Schema Registry log/work paths must be outside the shared Kafka installation")
			}
		}
	}
	for _, dir := range []string{s.Config, s.Log, s.Working, s.Plugin} {
		for _, tree := range []string{"bin", "libs", "share"} {
			if Overlaps(dir, s.Install+"/"+tree) {
				return s, fmt.Errorf("configuration/data paths must be separate from installed runtime tree %s", tree)
			}
		}
	}
	return s, nil
}

func heapMiB(value string) (int64, error) {
	if !heapPattern.MatchString(value) {
		return 0, fmt.Errorf("invalid heap size")
	}
	amount, err := strconv.ParseInt(value[:len(value)-1], 10, 64)
	if err != nil {
		return 0, err
	}
	if value[len(value)-1] == 'G' || value[len(value)-1] == 'g' {
		if amount > int64(^uint64(0)>>1)/1024 {
			return 0, fmt.Errorf("heap size is too large")
		}
		amount *= 1024
	}
	return amount, nil
}

func (s Settings) Directories() []string {
	dirs := []string{s.Install, s.Config, s.Log, s.Working}
	if s.Kind == Connect {
		dirs = append(dirs, s.Plugin)
	}
	return dirs
}

func Endpoints(value string) (string, error) {
	var endpoints []string
	for _, endpoint := range strings.Split(value, ",") {
		endpoint = strings.TrimSpace(endpoint)
		if strings.HasPrefix(endpoint, "PLAINTEXT://") {
			endpoint = strings.TrimPrefix(endpoint, "PLAINTEXT://")
		}
		host, port, err := net.SplitHostPort(endpoint)
		n, e := strconv.Atoi(port)
		if err != nil || e != nil || n < 1 || n > 65535 || !hostPattern.MatchString(host) {
			return "", fmt.Errorf("valid PLAINTEXT bootstrap host:port endpoints are required: %q", endpoint)
		}
		endpoints = append(endpoints, net.JoinHostPort(host, port))
	}
	return strings.Join(endpoints, ","), nil
}

// Only these exact shared paths are permitted. Kafka storage and all runtime
// trees remain protected, including paths redirected through symlinks.
func (s Settings) allowedSharedPath(dir, protected, kafkaRoot string) bool {
	if !s.SharedConnect && !s.SharedSchema {
		return false
	}
	// The existing shared root may contain the other service's protected config.
	// Individual service paths still cannot overlap one another.
	if dir == kafkaRoot && (protected == kafkaRoot+"/config" || protected == kafkaRoot+"/etc/schema-registry" || protected == kafkaRoot+"/connect-plugins") {
		return true
	}
	if protected != kafkaRoot {
		return false
	}
	return dir == kafkaRoot || (s.SharedConnect && (dir == kafkaRoot+"/config" || dir == kafkaRoot+"/connect-plugins")) ||
		(s.SharedSchema && dir == kafkaRoot+"/etc/schema-registry")
}
