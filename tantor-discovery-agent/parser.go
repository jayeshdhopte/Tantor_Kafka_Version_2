package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

func parseProperties(content string) map[string]string {
	props := make(map[string]string)
	var logical []string
	var current strings.Builder
	for _, raw := range strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n") {
		line := strings.TrimRight(raw, " \t")
		backslashes := 0
		for i := len(line) - 1; i >= 0 && line[i] == '\\'; i-- {
			backslashes++
		}
		continued := backslashes%2 == 1
		if continued {
			line = line[:len(line)-1]
		}
		if current.Len() > 0 {
			current.WriteString(strings.TrimLeft(line, " \t\f"))
		} else {
			current.WriteString(line)
		}
		if continued {
			continue
		}
		logical = append(logical, current.String())
		current.Reset()
	}
	if current.Len() > 0 {
		logical = append(logical, current.String())
	}
	for _, raw := range logical {
		line := strings.TrimLeft(raw, " \t\f")
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "!") {
			continue
		}
		sep := -1
		escaped := false
		for i, r := range line {
			if escaped {
				escaped = false
				continue
			}
			if r == '\\' {
				escaped = true
				continue
			}
			if r == '=' || r == ':' || r == ' ' || r == '\t' || r == '\f' {
				sep = i
				break
			}
		}
		var key, value string
		if sep < 0 {
			key = line
		} else {
			key = line[:sep]
			j := sep
			for j < len(line) && (line[j] == ' ' || line[j] == '\t' || line[j] == '\f') {
				j++
			}
			if j < len(line) && (line[j] == '=' || line[j] == ':') {
				j++
			}
			for j < len(line) && (line[j] == ' ' || line[j] == '\t' || line[j] == '\f') {
				j++
			}
			value = line[j:]
		}
		key = unescapeProperty(strings.TrimSpace(key))
		value = unescapeProperty(value)
		if key != "" {
			props[key] = value
		}
	}
	return props
}

func unescapeProperty(value string) string {
	var out strings.Builder
	escaped := false
	for _, r := range value {
		if !escaped {
			if r == '\\' {
				escaped = true
				continue
			}
			out.WriteRune(r)
			continue
		}
		switch r {
		case 't':
			out.WriteByte('\t')
		case 'n':
			out.WriteByte('\n')
		case 'r':
			out.WriteByte('\r')
		case 'f':
			out.WriteByte('\f')
		default:
			out.WriteRune(r)
		}
		escaped = false
	}
	if escaped {
		out.WriteByte('\\')
	}
	return out.String()
}

func normalizeProcessRoles(value, propsFile string) string {
	var out []string
	seen := map[string]bool{}
	for _, role := range strings.Split(strings.ToLower(strings.TrimSpace(value)), ",") {
		role = strings.TrimSpace(role)
		if (role == "broker" || role == "controller") && !seen[role] {
			seen[role] = true
			out = append(out, role)
		}
	}
	if len(out) == 0 {
		switch strings.ToLower(filepath.Base(propsFile)) {
		case "broker.properties":
			out = append(out, "broker")
		case "controller.properties":
			out = append(out, "controller")
		}
	}
	return strings.Join(out, ",")
}

func roleContains(roles, wanted string) bool {
	wanted = strings.ToLower(strings.TrimSpace(wanted))
	for _, role := range strings.Split(strings.ToLower(roles), ",") {
		if strings.TrimSpace(role) == wanted {
			return true
		}
	}
	return false
}

func parseServerProperties(propsFile string, isRunning bool, hostname, defaultEnv, clusterNameOverride string) *DiscoveredCluster {
	content, err := os.ReadFile(propsFile)
	if err != nil {
		return nil
	}
	props := parseProperties(string(content))
	processRoles := normalizeProcessRoles(props["process.roles"], propsFile)

	// Broker bootstrap endpoints come only from non-controller listeners. A
	// controller-only configuration is still a valid discovered node; its broker
	// bootstrap is filled from another node in the same KRaft cluster later.
	listenersRaw := firstNonBlank(props["advertised.listeners"], props["listeners"])
	bootstrap := extractBootstrapServersWithFallback(listenersRaw, hostname)
	if bootstrap == "" && !roleContains(processRoles, "controller") {
		return nil
	}

	installPath := findKafkaInstallRoot(propsFile)
	kafkaMode := "ZooKeeper"
	if processRoles != "" || props["controller.quorum.voters"] != "" || props["controller.quorum.bootstrap.servers"] != "" {
		kafkaMode = "KRaft"
	}

	nodeID := parseIntDefault(firstNonBlank(props["node.id"], props["broker.id"]), -1)
	dataDirs := strings.TrimSpace(firstNonBlank(props["log.dirs"], props["log.dir"], props["metadata.log.dir"]))
	metadataDir := strings.TrimSpace(props["metadata.log.dir"])

	clusterID := strings.TrimSpace(props["cluster.id"])
	if clusterID == "" {
		clusterID = readClusterIDFromLogs(strings.Join(nonBlankStrings(dataDirs, metadataDir), ","))
	}

	clusterName := strings.TrimSpace(clusterNameOverride)
	if clusterName == "" && clusterID != "" {
		clusterName = "kafka-" + sanitizeName(clusterID)
	}
	if clusterName == "" {
		clusterName = deriveClusterName(propsFile, hostname)
		if bootstrap != "" {
			clusterName += "-" + extractFirstPort(bootstrap)
		}
	}

	brokerCount := 0
	if roleContains(processRoles, "broker") {
		brokerCount = 1
	}

	return &DiscoveredCluster{
		Name:                clusterName,
		Hostname:            hostname,
		BootstrapServers:    bootstrap,
		KafkaClusterID:      clusterID,
		KafkaMode:           kafkaMode,
		ProcessRoles:        processRoles,
		Security:            detectSecurityForBrokerListeners(props["advertised.listeners"], props["listeners"], props["listener.security.protocol.map"]),
		BrokerCount:         brokerCount,
		NodeID:              nodeID,
		IsRunning:           isRunning,
		InstallPath:         installPath,
		PropsFile:           propsFile,
		DataDirs:            dataDirs,
		LogDirs:             dataDirs,
		Environment:         defaultEnv,
		Listeners:           props["listeners"],
		AdvertisedListeners: props["advertised.listeners"],
	}
}

func nonBlankStrings(values ...string) []string {
	var out []string
	seen := map[string]bool{}
	for _, value := range values {
		for _, item := range strings.Split(value, ",") {
			item = strings.TrimSpace(item)
			if item == "" || seen[item] {
				continue
			}
			seen[item] = true
			out = append(out, item)
		}
	}
	return out
}

func extractBootstrapServers(listenersStr string) string {
	return extractBootstrapServersWithFallback(listenersStr, "")
}

func isLoopbackOrWildcardHost(host string) bool {
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if host == "" || host == "0.0.0.0" || host == "::" || strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback() || ip.IsUnspecified()
	}
	return false
}

func extractBootstrapServersWithFallback(listenersStr, fallbackHost string) string {
	parts := strings.Split(listenersStr, ",")
	re := regexp.MustCompile(`://(\[[^\]]+\]|[^:]*):([0-9]+)`)
	seen := map[string]struct{}{}
	var brokers []string
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		upper := strings.ToUpper(trimmed)
		if strings.Contains(upper, "CONTROLLER") {
			continue
		}
		m := re.FindStringSubmatch(trimmed)
		if len(m) != 3 {
			continue
		}
		host := strings.TrimSpace(m[1])
		if isLoopbackOrWildcardHost(host) {
			host = strings.TrimSpace(fallbackHost)
		}
		if host == "" {
			continue
		}
		if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
			host = "[" + host + "]"
		}
		broker := host + ":" + m[2]
		if _, ok := seen[broker]; !ok {
			seen[broker] = struct{}{}
			brokers = append(brokers, broker)
		}
	}
	sort.Strings(brokers)
	return strings.Join(brokers, ",")
}

func parseListenerProtocolMap(value string) map[string]string {
	out := map[string]string{}
	for _, item := range strings.Split(value, ",") {
		parts := strings.SplitN(strings.TrimSpace(item), ":", 2)
		if len(parts) != 2 {
			continue
		}
		name := strings.ToUpper(strings.TrimSpace(parts[0]))
		protocol := strings.ToUpper(strings.TrimSpace(parts[1]))
		if name != "" && protocol != "" {
			out[name] = protocol
		}
	}
	return out
}

func protocolRank(protocol string) int {
	switch strings.ToUpper(protocol) {
	case "SASL_SSL":
		return 4
	case "SSL":
		return 3
	case "SASL_PLAINTEXT":
		return 2
	case "PLAINTEXT":
		return 1
	default:
		return 0
	}
}

func detectSecurityForBrokerListeners(advertised, listeners, protocolMap string) string {
	value := firstNonBlank(advertised, listeners)
	mapping := parseListenerProtocolMap(protocolMap)
	best := ""
	for _, listener := range strings.Split(value, ",") {
		listener = strings.TrimSpace(listener)
		if listener == "" {
			continue
		}
		idx := strings.Index(listener, "://")
		if idx <= 0 {
			continue
		}
		name := strings.ToUpper(strings.TrimSpace(listener[:idx]))
		if name == "CONTROLLER" || strings.Contains(name, "CONTROLLER") {
			continue
		}
		protocol := mapping[name]
		if protocol == "" {
			protocol = name
		}
		if protocolRank(protocol) > protocolRank(best) {
			best = protocol
		}
	}
	if protocolRank(best) == 0 {
		return "UNKNOWN"
	}
	return best
}

// Retained for compatibility with older tests/callers.
func detectSecurity(listenersStr, protocolMap string) string {
	return detectSecurityForBrokerListeners(listenersStr, listenersStr, protocolMap)
}

func sanitizeName(value string) string {
	re := regexp.MustCompile(`[^A-Za-z0-9_.-]+`)
	value = re.ReplaceAllString(strings.TrimSpace(value), "-")
	value = strings.Trim(value, "-.")
	if len(value) > 80 {
		value = value[:80]
	}
	if value == "" {
		return "unknown"
	}
	return value
}

func deriveClusterName(propsFile, hostname string) string {
	dir := filepath.Base(filepath.Dir(propsFile))
	return sanitizeName(hostname + "-" + dir)
}

func extractFirstPort(bootstrap string) string {
	first := strings.Split(bootstrap, ",")[0]
	idx := strings.LastIndex(first, ":")
	if idx >= 0 && idx < len(first)-1 {
		return first[idx+1:]
	}
	return "unknown"
}

func parseIntDefault(value string, fallback int) int {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	var parsed int
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil {
		return fallback
	}
	return parsed
}

func findKafkaInstallRoot(propsFile string) string {
	dir := filepath.Dir(propsFile)
	for i := 0; i < 8; i++ {
		if dirExists(filepath.Join(dir, "libs")) || dirExists(filepath.Join(dir, "bin")) {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return filepath.Dir(propsFile)
}

func dirExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func detectVersion(installPath string) string {
	entries, err := os.ReadDir(filepath.Join(installPath, "libs"))
	if err != nil {
		return "Unknown"
	}
	re1 := regexp.MustCompile(`kafka-clients-([0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.]+)?)\.jar`)
	for _, e := range entries {
		if m := re1.FindStringSubmatch(e.Name()); len(m) > 1 {
			return m[1]
		}
	}
	re2 := regexp.MustCompile(`kafka_[0-9.]+-([0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.]+)?)\.jar`)
	for _, e := range entries {
		if m := re2.FindStringSubmatch(e.Name()); len(m) > 1 {
			return m[1]
		}
	}
	return "Unknown"
}

func readClusterIDFromLogs(logDirsStr string) string {
	seen := map[string]bool{}
	for _, dir := range strings.Split(logDirsStr, ",") {
		dir = strings.TrimSpace(dir)
		if dir == "" || seen[dir] {
			continue
		}
		seen[dir] = true
		candidates := []string{
			filepath.Join(dir, "meta.properties"),
			filepath.Join(dir, "metadata", "meta.properties"),
		}
		for _, metaFile := range candidates {
			data, err := os.ReadFile(metaFile)
			if err != nil {
				continue
			}
			if id := strings.TrimSpace(parseProperties(string(data))["cluster.id"]); id != "" {
				return id
			}
		}
	}
	return ""
}

func detectEnvironment(hostname string) string {
	h := strings.ToLower(hostname)
	switch {
	case strings.Contains(h, "prod"):
		return "prod"
	case strings.Contains(h, "dev"):
		return "dev"
	case strings.Contains(h, "stag"), strings.Contains(h, "stg"):
		return "staging"
	case strings.Contains(h, "test"), strings.Contains(h, "qa"):
		return "test"
	case strings.Contains(h, "uat"):
		return "uat"
	default:
		return "unknown"
	}
}
