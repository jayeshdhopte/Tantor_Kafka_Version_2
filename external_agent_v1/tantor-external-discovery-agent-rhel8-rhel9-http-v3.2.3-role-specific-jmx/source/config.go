package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type RuntimeConfig struct {
	ServerURL               string
	HostID                  string
	AgentName               string
	NodeName                string
	ClusterName             string
	Environment             string
	ScanPaths               []string
	DiscoveryPolicy         string
	DiscoveryInterval       time.Duration
	TaskPollInterval        time.Duration
	MetricsInterval         time.Duration
	JMXMetricsURL           string
	ControllerJMXMetricsURL string
	KafkaExporterMetricsURL string
	HTTPTimeout             time.Duration
	HTTPRetries             int
	AuthType                string
	AuthUsername            string
	AuthPassword            string
	AuthPasswordFile        string
	AuthToken               string
	AuthTokenFile           string
	CAFile                  string
	ClientCertFile          string
	ClientKeyFile           string
	InsecureSkipVerify      bool
	BackupRoot              string
	EnableTasks             bool
	AllowConfigWrite        bool
	AllowServiceRestart     bool
	RestartWithSudo         bool
	AllowedServices         []string
	RunPrecheck             bool
	PrecheckOnly            bool
	PrecheckJSON            bool
	Once                    bool
	LogLevel                string
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return strings.TrimSpace(v)
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return fallback
	}
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return b
}

func envInt(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return fallback
	}
	i, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return i
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return fallback
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return d
}

func splitCSV(value string) []string {
	var out []string
	seen := map[string]struct{}{}
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func detectOutboundNodeName(serverURL, fallback string) string {
	fallback = strings.TrimSpace(fallback)
	u, err := url.Parse(strings.TrimSpace(serverURL))
	if err == nil && u.Hostname() != "" {
		port := u.Port()
		if port == "" {
			port = "80"
		}
		conn, err := net.Dial("udp", net.JoinHostPort(u.Hostname(), port))
		if err == nil {
			defer conn.Close()
			if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok && addr.IP != nil && !addr.IP.IsLoopback() && !addr.IP.IsUnspecified() {
				return addr.IP.String()
			}
		}
	}

	lower := strings.ToLower(fallback)
	if fallback != "" && lower != "localhost" && !strings.HasPrefix(lower, "localhost.") {
		return fallback
	}

	// Last-resort: choose the first non-loopback address from an active local interface.
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, addr := range addrs {
			var ip net.IP
			switch value := addr.(type) {
			case *net.IPNet:
				ip = value.IP
			case *net.IPAddr:
				ip = value.IP
			}
			if ip != nil && ip.To4() != nil && !ip.IsLoopback() && !ip.IsUnspecified() {
				return ip.String()
			}
		}
	}
	return firstNonBlank(fallback, "unknown-host")
}

func defaultHostID(hostname string) string {
	machineID, _ := os.ReadFile("/etc/machine-id")
	seed := strings.TrimSpace(string(machineID))
	if seed == "" {
		seed = hostname
	}
	sum := sha256.Sum256([]byte(seed))
	return "host-" + hex.EncodeToString(sum[:8])
}

func LoadRuntimeConfig(args []string) (RuntimeConfig, error) {
	hostname, _ := os.Hostname()
	if strings.TrimSpace(hostname) == "" {
		hostname = "unknown-host"
	}

	fs := flag.NewFlagSet("tantor-discovery-agent", flag.ContinueOnError)
	serverURL := fs.String("server-url", env("TANTOR_AGENT_SERVER_URL", ""), "Tantor backend base URL")
	hostID := fs.String("host-id", env("TANTOR_AGENT_HOST_ID", ""), "Stable host ID; auto-generated when empty")
	agentName := fs.String("agent-name", env("TANTOR_AGENT_NAME", ""), "Agent name; defaults to tantor-agent-<hostname>")
	nodeName := fs.String("node-name", env("TANTOR_AGENT_NODE_NAME", ""), "Node name; defaults to OS hostname")
	clusterName := fs.String("cluster-name", env("TANTOR_AGENT_CLUSTER_NAME", ""), "Optional stable cluster name override")
	environment := fs.String("environment", env("TANTOR_AGENT_ENVIRONMENT", ""), "Environment label such as prod or uat")
	scanPaths := fs.String("scan-paths", env("TANTOR_AGENT_SCAN_PATHS", ""), "Comma-separated explicit Kafka scan roots (required)")
	discoveryPolicy := fs.String("discovery-policy", env("TANTOR_AGENT_DISCOVERY_POLICY", "running-only"), "Discovery policy: running-only, running-with-offline-inventory, filesystem-only")
	discoveryInterval := fs.Duration("discovery-interval", envDuration("TANTOR_AGENT_DISCOVERY_INTERVAL", 30*time.Second), "Kafka discovery interval")
	taskPollInterval := fs.Duration("task-poll-interval", envDuration("TANTOR_AGENT_TASK_POLL_INTERVAL", 5*time.Second), "Task poll interval")
	metricsInterval := fs.Duration("metrics-interval", envDuration("TANTOR_AGENT_METRICS_INTERVAL", 15*time.Second), "Metrics publish interval")
	jmxURL := fs.String("jmx-metrics-url", env("TANTOR_AGENT_JMX_METRICS_URL", ""), "Optional JMX Exporter HTTP /metrics URL; supports {host}")
	controllerJMXURL := fs.String("controller-jmx-metrics-url", env("TANTOR_AGENT_CONTROLLER_JMX_METRICS_URL", ""), "Optional controller-only JMX Exporter HTTP /metrics URL; supports {host}")
	kafkaExporterURL := fs.String("kafka-exporter-metrics-url", env("TANTOR_AGENT_KAFKA_EXPORTER_METRICS_URL", ""), "Optional Kafka Exporter HTTP /metrics URL")
	httpTimeout := fs.Duration("http-timeout", envDuration("TANTOR_AGENT_HTTP_TIMEOUT", 20*time.Second), "Per-request HTTP timeout")
	httpRetries := fs.Int("http-retries", envInt("TANTOR_AGENT_HTTP_RETRIES", 3), "Retry count for temporary server/network failures")
	authType := fs.String("auth-type", env("TANTOR_AGENT_AUTH_TYPE", "none"), "Authentication: none, bearer, basic, or mtls")
	authUsername := fs.String("auth-username", env("TANTOR_AGENT_AUTH_USERNAME", ""), "Basic-auth username")
	authPasswordFile := fs.String("auth-password-file", env("TANTOR_AGENT_AUTH_PASSWORD_FILE", ""), "File containing the basic-auth password")
	authTokenFile := fs.String("auth-token-file", env("TANTOR_AGENT_AUTH_TOKEN_FILE", ""), "File containing the bearer token")
	caFile := fs.String("ca-file", env("TANTOR_AGENT_CA_FILE", ""), "PEM CA bundle for server verification")
	clientCert := fs.String("client-cert", env("TANTOR_AGENT_CLIENT_CERT", ""), "PEM client certificate for mTLS")
	clientKey := fs.String("client-key", env("TANTOR_AGENT_CLIENT_KEY", ""), "PEM client key for mTLS")
	insecure := fs.Bool("insecure-skip-verify", envBool("TANTOR_AGENT_INSECURE_SKIP_VERIFY", false), "Disable TLS certificate verification (not recommended)")
	backupRoot := fs.String("backup-root", env("TANTOR_AGENT_BACKUP_ROOT", "/var/lib/tantor-agent/backups"), "Agent-controlled backup root")
	enableTasks := fs.Bool("enable-tasks", envBool("TANTOR_AGENT_ENABLE_TASKS", false), "Enable server task polling")
	allowConfigWrite := fs.Bool("allow-config-write", envBool("TANTOR_AGENT_ALLOW_CONFIG_WRITE", false), "Allow server-requested Kafka config changes")
	allowRestart := fs.Bool("allow-service-restart", envBool("TANTOR_AGENT_ALLOW_SERVICE_RESTART", false), "Allow server-requested service restart")
	restartWithSudo := fs.Bool("restart-with-sudo", envBool("TANTOR_AGENT_RESTART_WITH_SUDO", false), "Use sudo -n for allowed service restarts")
	allowedServices := fs.String("allowed-services", env("TANTOR_AGENT_ALLOWED_SERVICES", ""), "Comma-separated systemd service allowlist")
	runPrecheck := fs.Bool("precheck", envBool("TANTOR_AGENT_RUN_PRECHECK", true), "Run local Kafka precheck at startup")
	precheckOnly := fs.Bool("precheck-only", false, "Run precheck and exit")
	precheckJSON := fs.Bool("precheck-json", false, "Print precheck JSON and exit")
	once := fs.Bool("once", false, "Run one discovery cycle and exit")
	logLevel := fs.String("log-level", env("TANTOR_AGENT_LOG_LEVEL", "info"), "Log level: debug, info, warn, error")

	if err := fs.Parse(args); err != nil {
		return RuntimeConfig{}, err
	}
	explicitNodeName := strings.TrimSpace(*nodeName) != ""

	cfg := RuntimeConfig{
		ServerURL:               strings.TrimRight(strings.TrimSpace(*serverURL), "/"),
		HostID:                  strings.TrimSpace(*hostID),
		AgentName:               strings.TrimSpace(*agentName),
		NodeName:                strings.TrimSpace(*nodeName),
		ClusterName:             strings.TrimSpace(*clusterName),
		Environment:             strings.TrimSpace(*environment),
		ScanPaths:               splitCSV(*scanPaths),
		DiscoveryPolicy:         strings.ToLower(strings.TrimSpace(*discoveryPolicy)),
		DiscoveryInterval:       *discoveryInterval,
		TaskPollInterval:        *taskPollInterval,
		MetricsInterval:         *metricsInterval,
		JMXMetricsURL:           strings.TrimSpace(*jmxURL),
		ControllerJMXMetricsURL: strings.TrimSpace(*controllerJMXURL),
		KafkaExporterMetricsURL: strings.TrimSpace(*kafkaExporterURL),
		HTTPTimeout:             *httpTimeout,
		HTTPRetries:             *httpRetries,
		AuthType:                strings.ToLower(strings.TrimSpace(*authType)),
		AuthUsername:            strings.TrimSpace(*authUsername),
		AuthPassword:            env("TANTOR_AGENT_AUTH_PASSWORD", ""),
		AuthPasswordFile:        strings.TrimSpace(*authPasswordFile),
		AuthToken:               env("TANTOR_AGENT_AUTH_TOKEN", ""),
		AuthTokenFile:           strings.TrimSpace(*authTokenFile),
		CAFile:                  strings.TrimSpace(*caFile),
		ClientCertFile:          strings.TrimSpace(*clientCert),
		ClientKeyFile:           strings.TrimSpace(*clientKey),
		InsecureSkipVerify:      *insecure,
		BackupRoot:              strings.TrimSpace(*backupRoot),
		EnableTasks:             *enableTasks,
		AllowConfigWrite:        *allowConfigWrite,
		AllowServiceRestart:     *allowRestart,
		RestartWithSudo:         *restartWithSudo,
		AllowedServices:         splitCSV(*allowedServices),
		RunPrecheck:             *runPrecheck,
		PrecheckOnly:            *precheckOnly,
		PrecheckJSON:            *precheckJSON,
		Once:                    *once,
		LogLevel:                strings.ToLower(strings.TrimSpace(*logLevel)),
	}

	if cfg.NodeName == "" {
		cfg.NodeName = strings.TrimSpace(hostname)
	}
	if cfg.HostID == "" {
		// Host ID remains machine based and does not change when an interface/IP changes.
		cfg.HostID = defaultHostID(hostname)
	}
	if cfg.AgentName == "" {
		cfg.AgentName = "tantor-agent-" + cfg.NodeName
	}

	if cfg.PrecheckOnly || cfg.PrecheckJSON {
		return cfg, nil
	}
	if cfg.ServerURL == "" {
		return RuntimeConfig{}, errors.New("server URL is required; use --server-url or TANTOR_AGENT_SERVER_URL")
	}
	u, err := url.Parse(cfg.ServerURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return RuntimeConfig{}, fmt.Errorf("invalid server URL %q: expected http://host:port or https://host:port", cfg.ServerURL)
	}
	if u.User != nil {
		return RuntimeConfig{}, errors.New("server URL must not contain embedded credentials; use the auth options instead")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return RuntimeConfig{}, errors.New("server URL must not contain a query string or fragment")
	}
	if cfg.HTTPTimeout <= 0 {
		return RuntimeConfig{}, errors.New("http-timeout must be greater than zero")
	}
	if cfg.HTTPRetries < 0 || cfg.HTTPRetries > 10 {
		return RuntimeConfig{}, errors.New("http-retries must be between 0 and 10")
	}
	if cfg.DiscoveryInterval <= 0 && !cfg.Once {
		return RuntimeConfig{}, errors.New("discovery-interval must be greater than zero")
	}
	if cfg.EnableTasks && cfg.TaskPollInterval <= 0 {
		return RuntimeConfig{}, errors.New("task-poll-interval must be greater than zero")
	}
	if cfg.MetricsInterval <= 0 {
		return RuntimeConfig{}, errors.New("metrics-interval must be greater than zero")
	}
	for name, endpoint := range map[string]string{
		"jmx-metrics-url":            cfg.JMXMetricsURL,
		"controller-jmx-metrics-url": cfg.ControllerJMXMetricsURL,
		"kafka-exporter-metrics-url": cfg.KafkaExporterMetricsURL,
	} {
		if endpoint == "" {
			continue
		}
		u, err := url.Parse(endpoint)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return RuntimeConfig{}, fmt.Errorf("invalid %s %q: expected an HTTP(S) exporter endpoint", name, endpoint)
		}
		if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return RuntimeConfig{}, fmt.Errorf("%s must not contain credentials, query parameters or a fragment", name)
		}
		if strings.Contains(endpoint, "{port}") {
			return RuntimeConfig{}, fmt.Errorf("%s must use the explicit exporter port; {port} cannot be derived from the Kafka listener", name)
		}
	}
	if len(cfg.ScanPaths) == 0 {
		return RuntimeConfig{}, errors.New("explicit scan paths are required; use --scan-paths or TANTOR_AGENT_SCAN_PATHS")
	}
	if strings.TrimSpace(cfg.Environment) == "" {
		return RuntimeConfig{}, errors.New("environment is required for production onboarding")
	}
	if !explicitNodeName || strings.TrimSpace(cfg.NodeName) == "" || cfg.NodeName == "unknown-host" {
		return RuntimeConfig{}, errors.New("explicit node-name is required and must identify this Kafka VM")
	}
	validPolicies := map[string]bool{"running-only": true, "running-with-offline-inventory": true, "filesystem-only": true}
	if !validPolicies[cfg.DiscoveryPolicy] {
		return RuntimeConfig{}, fmt.Errorf("unsupported discovery-policy %q", cfg.DiscoveryPolicy)
	}
	for i, p := range cfg.ScanPaths {
		if !filepath.IsAbs(p) {
			return RuntimeConfig{}, fmt.Errorf("scan path must be absolute: %s", p)
		}
		cleaned := filepath.Clean(p)
		if cleaned == "/" {
			return RuntimeConfig{}, errors.New("scan path / is not allowed")
		}
		cfg.ScanPaths[i] = cleaned
	}
	if !filepath.IsAbs(cfg.BackupRoot) {
		return RuntimeConfig{}, errors.New("backup-root must be an absolute path")
	}
	cfg.BackupRoot = filepath.Clean(cfg.BackupRoot)

	validAuth := map[string]bool{"none": true, "bearer": true, "basic": true, "mtls": true}
	if !validAuth[cfg.AuthType] {
		return RuntimeConfig{}, fmt.Errorf("unsupported auth type %q", cfg.AuthType)
	}
	if cfg.AuthType == "bearer" && cfg.AuthToken == "" && cfg.AuthTokenFile == "" {
		return RuntimeConfig{}, errors.New("bearer auth requires --auth-token-file or TANTOR_AGENT_AUTH_TOKEN")
	}
	if cfg.AuthType == "basic" {
		if cfg.AuthUsername == "" {
			return RuntimeConfig{}, errors.New("basic auth requires --auth-username")
		}
		if cfg.AuthPassword == "" && cfg.AuthPasswordFile == "" {
			return RuntimeConfig{}, errors.New("basic auth requires --auth-password-file or TANTOR_AGENT_AUTH_PASSWORD")
		}
	}
	if cfg.AuthType == "mtls" && (cfg.ClientCertFile == "" || cfg.ClientKeyFile == "") {
		return RuntimeConfig{}, errors.New("mtls auth requires --client-cert and --client-key")
	}

	unitRE := regexp.MustCompile(`^[A-Za-z0-9_.@:-]+\.service$`)
	for _, service := range cfg.AllowedServices {
		if !unitRE.MatchString(service) {
			return RuntimeConfig{}, fmt.Errorf("invalid systemd service name in allowlist: %s", service)
		}
	}
	if cfg.AllowServiceRestart && len(cfg.AllowedServices) == 0 {
		return RuntimeConfig{}, errors.New("service restart is enabled but allowed-services is empty")
	}
	return cfg, nil
}
