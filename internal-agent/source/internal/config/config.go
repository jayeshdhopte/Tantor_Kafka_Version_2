package config

import (
	"bufio"
	"crypto/rand"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Config is intentionally composed only of standard-library types so the agent
// can be built in a fully air-gapped environment without downloading modules.
type Config struct {
	Agent     AgentConfig
	Paths     PathConfig
	Auth      AuthConfig
	HTTP      HTTPConfig
	Privilege PrivilegeConfig
}

type AgentConfig struct {
	HostID             string
	AgentName          string
	ServerURL          string
	CertFile           string
	KeyFile            string
	CACert             string
	InsecureSkipVerify bool
	PollInterval       int
	HeartbeatInterval  int
	LogLevel           string
}

type PathConfig struct {
	DataDir                string
	LogDir                 string
	ArtifactsDir           string
	AllowedDeploymentRoots string
}

type AuthConfig struct {
	Mode         string
	Token        string
	TokenFile    string
	Username     string
	Password     string
	PasswordFile string
}

type HTTPConfig struct {
	RequestTimeoutSeconds  int
	ArtifactTimeoutSeconds int
	DialTimeoutSeconds     int
	TLSHandshakeSeconds    int
	IdleConnTimeoutSeconds int
	UseEnvironmentProxy    bool
}

type PrivilegeConfig struct {
	Mode     string
	SudoPath string
}

func defaultConfig() Config {
	var cfg Config
	cfg.Agent.PollInterval = 10
	cfg.Agent.HeartbeatInterval = 30
	cfg.Agent.LogLevel = "INFO"
	cfg.Paths.DataDir = "/var/lib/tantor-agent/data"
	cfg.Paths.LogDir = "/var/log/tantor-agent"
	cfg.Paths.ArtifactsDir = "/var/lib/tantor-agent/artifacts"
	cfg.Paths.AllowedDeploymentRoots = "/var/lib/tantor-agent/artifacts:/opt/tantor-kafka:/var/lib/tantor-kafka"
	cfg.Auth.Mode = "none"
	cfg.HTTP.RequestTimeoutSeconds = 600
	cfg.HTTP.ArtifactTimeoutSeconds = 1800
	cfg.HTTP.DialTimeoutSeconds = 10
	cfg.HTTP.TLSHandshakeSeconds = 10
	cfg.HTTP.IdleConnTimeoutSeconds = 90
	cfg.Privilege.Mode = "sudo"
	cfg.Privilege.SudoPath = "/usr/bin/sudo"
	return cfg
}

// Load reads the supported YAML subset used by the bundled configuration file,
// applies TANTOR_* environment overrides, resolves relative credential paths,
// validates the result, and creates a stable host ID when one was not supplied.
func Load(path string) (*Config, error) {
	cfg := defaultConfig()

	if path != "" {
		if err := parseFile(path, &cfg); err != nil {
			return nil, err
		}
	}
	if err := applyEnvironment(&cfg); err != nil {
		return nil, err
	}
	resolveRelativePaths(filepath.Dir(path), &cfg)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := cfg.ensureRuntimeIdentity(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func parseFile(path string, cfg *Config) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("read config %q: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// Configuration files should be small, but raise the scanner cap to make
	// accidental long certificate paths/comments fail less surprisingly.
	scanner.Buffer(make([]byte, 4096), 1024*1024)

	section := ""
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		raw := strings.TrimSuffix(scanner.Text(), "\r")
		line := strings.TrimSpace(stripInlineComment(raw))
		if line == "" {
			continue
		}

		indent := len(raw) - len(strings.TrimLeft(raw, " \t"))
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			return fmt.Errorf("config line %d: expected key: value", lineNo)
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" {
			return fmt.Errorf("config line %d: empty key", lineNo)
		}

		if indent == 0 && value == "" {
			section = strings.ToLower(key)
			continue
		}
		if section == "" {
			return fmt.Errorf("config line %d: key %q is not inside a section", lineNo, key)
		}

		parsed, err := parseScalar(value)
		if err != nil {
			return fmt.Errorf("config line %d: %w", lineNo, err)
		}
		if err := setField(cfg, section, strings.ToLower(key), parsed); err != nil {
			return fmt.Errorf("config line %d: %w", lineNo, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan config %q: %w", path, err)
	}
	return nil
}

func stripInlineComment(s string) string {
	var quote rune
	escaped := false
	for i, r := range s {
		if escaped {
			escaped = false
			continue
		}
		if r == '\\' && quote == '"' {
			escaped = true
			continue
		}
		if quote != 0 {
			if r == quote {
				quote = 0
			}
			continue
		}
		if r == '\'' || r == '"' {
			quote = r
			continue
		}
		if r == '#' {
			return s[:i]
		}
	}
	return s
}

func parseScalar(v string) (string, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return "", nil
	}
	if strings.HasPrefix(v, "\"") {
		out, err := strconv.Unquote(v)
		if err != nil {
			return "", fmt.Errorf("invalid quoted value: %w", err)
		}
		return out, nil
	}
	if strings.HasPrefix(v, "'") {
		if len(v) < 2 || !strings.HasSuffix(v, "'") {
			return "", errors.New("unterminated single-quoted value")
		}
		return strings.ReplaceAll(v[1:len(v)-1], "''", "'"), nil
	}
	return v, nil
}

func setField(cfg *Config, section, key, value string) error {
	switch section + "." + key {
	case "agent.host_id":
		cfg.Agent.HostID = value
	case "agent.agent_name":
		cfg.Agent.AgentName = value
	case "agent.server_url":
		cfg.Agent.ServerURL = value
	case "agent.cert_file":
		cfg.Agent.CertFile = value
	case "agent.key_file":
		cfg.Agent.KeyFile = value
	case "agent.ca_cert":
		cfg.Agent.CACert = value
	case "agent.insecure_skip_verify":
		return parseBoolInto(value, &cfg.Agent.InsecureSkipVerify)
	case "agent.poll_interval_seconds":
		return parseIntInto(value, &cfg.Agent.PollInterval)
	case "agent.heartbeat_interval_seconds":
		return parseIntInto(value, &cfg.Agent.HeartbeatInterval)
	case "agent.log_level":
		cfg.Agent.LogLevel = value
	case "paths.data_dir":
		cfg.Paths.DataDir = value
	case "paths.log_dir":
		cfg.Paths.LogDir = value
	case "paths.artifacts_dir":
		cfg.Paths.ArtifactsDir = value
	case "paths.allowed_deployment_roots":
		cfg.Paths.AllowedDeploymentRoots = value
	case "auth.mode":
		cfg.Auth.Mode = value
	case "auth.token":
		cfg.Auth.Token = value
	case "auth.token_file":
		cfg.Auth.TokenFile = value
	case "auth.username":
		cfg.Auth.Username = value
	case "auth.password":
		cfg.Auth.Password = value
	case "auth.password_file":
		cfg.Auth.PasswordFile = value
	case "http.request_timeout_seconds":
		return parseIntInto(value, &cfg.HTTP.RequestTimeoutSeconds)
	case "http.artifact_timeout_seconds":
		return parseIntInto(value, &cfg.HTTP.ArtifactTimeoutSeconds)
	case "http.dial_timeout_seconds":
		return parseIntInto(value, &cfg.HTTP.DialTimeoutSeconds)
	case "http.tls_handshake_timeout_seconds":
		return parseIntInto(value, &cfg.HTTP.TLSHandshakeSeconds)
	case "http.idle_conn_timeout_seconds":
		return parseIntInto(value, &cfg.HTTP.IdleConnTimeoutSeconds)
	case "http.use_environment_proxy":
		return parseBoolInto(value, &cfg.HTTP.UseEnvironmentProxy)
	case "privilege.mode":
		cfg.Privilege.Mode = value
	case "privilege.sudo_path":
		cfg.Privilege.SudoPath = value
	default:
		return fmt.Errorf("unsupported configuration key %s.%s", section, key)
	}
	return nil
}

func parseIntInto(value string, dest *int) error {
	n, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return fmt.Errorf("expected integer, got %q", value)
	}
	*dest = n
	return nil
}

func parseBoolInto(value string, dest *bool) error {
	b, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return fmt.Errorf("expected boolean, got %q", value)
	}
	*dest = b
	return nil
}

func applyEnvironment(cfg *Config) error {
	stringOverrides := map[string]*string{
		"TANTOR_HOST_ID":                  &cfg.Agent.HostID,
		"TANTOR_AGENT_NAME":               &cfg.Agent.AgentName,
		"TANTOR_SERVER_URL":               &cfg.Agent.ServerURL,
		"TANTOR_TLS_CERT_FILE":            &cfg.Agent.CertFile,
		"TANTOR_TLS_KEY_FILE":             &cfg.Agent.KeyFile,
		"TANTOR_TLS_CA_CERT":              &cfg.Agent.CACert,
		"TANTOR_LOG_LEVEL":                &cfg.Agent.LogLevel,
		"TANTOR_DATA_DIR":                 &cfg.Paths.DataDir,
		"TANTOR_LOG_DIR":                  &cfg.Paths.LogDir,
		"TANTOR_ARTIFACTS_DIR":            &cfg.Paths.ArtifactsDir,
		"TANTOR_ALLOWED_DEPLOYMENT_ROOTS": &cfg.Paths.AllowedDeploymentRoots,
		"TANTOR_AUTH_MODE":                &cfg.Auth.Mode,
		"TANTOR_AUTH_TOKEN":               &cfg.Auth.Token,
		"TANTOR_AUTH_TOKEN_FILE":          &cfg.Auth.TokenFile,
		"TANTOR_AUTH_USERNAME":            &cfg.Auth.Username,
		"TANTOR_AUTH_PASSWORD":            &cfg.Auth.Password,
		"TANTOR_AUTH_PASSWORD_FILE":       &cfg.Auth.PasswordFile,
		"TANTOR_PRIVILEGE_MODE":           &cfg.Privilege.Mode,
		"TANTOR_SUDO_PATH":                &cfg.Privilege.SudoPath,
	}
	for name, dest := range stringOverrides {
		if value, ok := os.LookupEnv(name); ok {
			*dest = value
		}
	}

	intOverrides := map[string]*int{
		"TANTOR_POLL_INTERVAL_SECONDS":         &cfg.Agent.PollInterval,
		"TANTOR_HEARTBEAT_INTERVAL_SECONDS":    &cfg.Agent.HeartbeatInterval,
		"TANTOR_HTTP_REQUEST_TIMEOUT_SECONDS":  &cfg.HTTP.RequestTimeoutSeconds,
		"TANTOR_ARTIFACT_TIMEOUT_SECONDS":      &cfg.HTTP.ArtifactTimeoutSeconds,
		"TANTOR_DIAL_TIMEOUT_SECONDS":          &cfg.HTTP.DialTimeoutSeconds,
		"TANTOR_TLS_HANDSHAKE_TIMEOUT_SECONDS": &cfg.HTTP.TLSHandshakeSeconds,
		"TANTOR_IDLE_CONN_TIMEOUT_SECONDS":     &cfg.HTTP.IdleConnTimeoutSeconds,
	}
	for name, dest := range intOverrides {
		if value, ok := os.LookupEnv(name); ok {
			if err := parseIntInto(value, dest); err != nil {
				return fmt.Errorf("environment %s: %w", name, err)
			}
		}
	}

	boolOverrides := map[string]*bool{
		"TANTOR_TLS_INSECURE_SKIP_VERIFY": &cfg.Agent.InsecureSkipVerify,
		"TANTOR_USE_ENVIRONMENT_PROXY":    &cfg.HTTP.UseEnvironmentProxy,
	}
	for name, dest := range boolOverrides {
		if value, ok := os.LookupEnv(name); ok {
			if err := parseBoolInto(value, dest); err != nil {
				return fmt.Errorf("environment %s: %w", name, err)
			}
		}
	}
	return nil
}

func resolveRelativePaths(configDir string, cfg *Config) {
	if configDir == "" || configDir == "." {
		return
	}
	for _, dest := range []*string{
		&cfg.Agent.CertFile,
		&cfg.Agent.KeyFile,
		&cfg.Agent.CACert,
		&cfg.Auth.TokenFile,
		&cfg.Auth.PasswordFile,
	} {
		if *dest != "" && !filepath.IsAbs(*dest) {
			*dest = filepath.Clean(filepath.Join(configDir, *dest))
		}
	}
}

// Validate catches deployment-time mistakes before the long-running service starts.
func (c *Config) Validate() error {
	c.Agent.ServerURL = strings.TrimRight(strings.TrimSpace(c.Agent.ServerURL), "/")
	if c.Agent.ServerURL == "" {
		return errors.New("agent.server_url is required")
	}
	u, err := url.Parse(c.Agent.ServerURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("agent.server_url must be an absolute http(s) URL: %q", c.Agent.ServerURL)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("agent.server_url scheme must be http or https, got %q", u.Scheme)
	}
	if u.User != nil {
		return errors.New("agent.server_url must not contain embedded credentials; configure auth.* instead")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return errors.New("agent.server_url must not contain a query string or fragment")
	}
	if c.Agent.PollInterval < 1 || c.Agent.PollInterval > 3600 {
		return fmt.Errorf("agent.poll_interval_seconds must be between 1 and 3600")
	}
	if c.Agent.HeartbeatInterval < 5 || c.Agent.HeartbeatInterval > 3600 {
		return fmt.Errorf("agent.heartbeat_interval_seconds must be between 5 and 3600")
	}
	for name, value := range map[string]int{
		"http.request_timeout_seconds":       c.HTTP.RequestTimeoutSeconds,
		"http.artifact_timeout_seconds":      c.HTTP.ArtifactTimeoutSeconds,
		"http.dial_timeout_seconds":          c.HTTP.DialTimeoutSeconds,
		"http.tls_handshake_timeout_seconds": c.HTTP.TLSHandshakeSeconds,
		"http.idle_conn_timeout_seconds":     c.HTTP.IdleConnTimeoutSeconds,
	} {
		if value < 1 {
			return fmt.Errorf("%s must be greater than zero", name)
		}
	}

	if (c.Agent.CertFile == "") != (c.Agent.KeyFile == "") {
		return errors.New("agent.cert_file and agent.key_file must be configured together")
	}
	if u.Scheme == "http" && (c.Agent.CertFile != "" || c.Agent.CACert != "" || c.Agent.InsecureSkipVerify) {
		return errors.New("TLS settings were supplied but agent.server_url uses http")
	}

	c.Auth.Mode = strings.ToLower(strings.TrimSpace(c.Auth.Mode))
	switch c.Auth.Mode {
	case "", "none":
		c.Auth.Mode = "none"
	case "bearer":
		if strings.TrimSpace(c.Auth.Token) == "" && strings.TrimSpace(c.Auth.TokenFile) == "" {
			return errors.New("auth.mode=bearer requires auth.token or auth.token_file")
		}
	case "basic":
		if strings.TrimSpace(c.Auth.Username) == "" {
			return errors.New("auth.mode=basic requires auth.username")
		}
		if c.Auth.Password == "" && c.Auth.PasswordFile == "" {
			return errors.New("auth.mode=basic requires auth.password or auth.password_file")
		}
	default:
		return fmt.Errorf("unsupported auth.mode %q; expected none, bearer, or basic", c.Auth.Mode)
	}

	c.Privilege.Mode = strings.ToLower(strings.TrimSpace(c.Privilege.Mode))
	switch c.Privilege.Mode {
	case "sudo":
		if strings.TrimSpace(c.Privilege.SudoPath) == "" {
			return errors.New("privilege.sudo_path is required when privilege.mode=sudo")
		}
	case "direct":
	default:
		return fmt.Errorf("unsupported privilege.mode %q; expected sudo or direct", c.Privilege.Mode)
	}

	for name, value := range map[string]string{
		"paths.data_dir":      c.Paths.DataDir,
		"paths.log_dir":       c.Paths.LogDir,
		"paths.artifacts_dir": c.Paths.ArtifactsDir,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s must not be empty", name)
		}
		if !filepath.IsAbs(value) {
			return fmt.Errorf("%s must be an absolute path, got %q", name, value)
		}
	}
	for _, root := range strings.Split(c.Paths.AllowedDeploymentRoots, ":") {
		root = filepath.Clean(strings.TrimSpace(root))
		if root == "." || !filepath.IsAbs(root) {
			return fmt.Errorf("paths.allowed_deployment_roots contains a non-absolute path %q", root)
		}
		switch root {
		case "/", "/etc", "/opt", "/var", "/var/lib", "/home", "/root", "/usr", "/data":
			return fmt.Errorf("paths.allowed_deployment_roots contains protected or overly broad root %q", root)
		}
	}
	return nil
}

func (c *Config) ensureRuntimeIdentity() error {
	if strings.TrimSpace(c.Agent.AgentName) == "" {
		hostname, err := os.Hostname()
		if err != nil || strings.TrimSpace(hostname) == "" {
			c.Agent.AgentName = "tantor-agent"
		} else {
			c.Agent.AgentName = hostname
		}
	}
	if strings.TrimSpace(c.Agent.HostID) != "" {
		return nil
	}
	if err := os.MkdirAll(c.Paths.DataDir, 0o750); err != nil {
		return fmt.Errorf("create data directory for stable host ID: %w", err)
	}
	idPath := filepath.Join(c.Paths.DataDir, "host-id")
	if data, err := os.ReadFile(idPath); err == nil {
		id := strings.TrimSpace(string(data))
		if id == "" {
			return fmt.Errorf("stable host ID file %q is empty", idPath)
		}
		c.Agent.HostID = id
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read stable host ID %q: %w", idPath, err)
	}

	id, err := newUUIDv4()
	if err != nil {
		return fmt.Errorf("generate stable host ID: %w", err)
	}
	tmp := idPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(id+"\n"), 0o600); err != nil {
		return fmt.Errorf("write stable host ID temp file: %w", err)
	}
	if err := os.Rename(tmp, idPath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("persist stable host ID: %w", err)
	}
	c.Agent.HostID = id
	return nil
}

func newUUIDv4() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
