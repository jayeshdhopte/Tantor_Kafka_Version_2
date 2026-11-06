// Package expansion contains the frozen controller/separate-process execution contract.
// It is distinct from legacy broker plans so their receipts and resume identity stay unchanged.
package expansion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"io.translab/tantor-agent/internal/taskvalidate"
)

func dispatchContext(ctx context.Context, params map[string]string) (context.Context, context.CancelFunc, error) {
	expires, err := time.Parse(time.RFC3339Nano, params["dispatch_expires_at"])
	now := time.Now()
	if err != nil || !expires.After(now) || expires.After(now.Add(125*time.Second)) {
		return nil, nil, fmt.Errorf("fresh bounded expansion dispatch deadline required")
	}
	bounded, cancel := context.WithDeadline(ctx, expires)
	return bounded, cancel, nil
}

type Runtime struct {
	JmxArtifactID string `json:"jmxArtifactId"`
	JmxChecksum   string `json:"jmxChecksum"`
	JmxPort       int    `json:"jmxPort"`
	LimitNofile   int    `json:"limitNofile"`
}
type ResourcePolicy struct {
	MinAvailableMemoryMB uint64 `json:"minAvailableMemoryMb"`
	OSReserveMB          uint64 `json:"osReserveMb"`
	MinDiskGB            uint64 `json:"minDiskGb"`
	MinInodes            uint64 `json:"minInodes"`
	AllowedJavaMajors    []int  `json:"allowedJavaMajors"`
}
type Process struct {
	Role         string            `json:"role"`
	NodeID       int               `json:"nodeId"`
	Port         int               `json:"port"`
	DataPaths    []string          `json:"dataPaths"`
	MetadataPath string            `json:"metadataPath"`
	LogPath      string            `json:"logPath"`
	HeapXmx      string            `json:"heapXmx"`
	BrokerRack   string            `json:"brokerRack"`
	Inherited    map[string]string `json:"inherited"`
	Runtime      Runtime           `json:"runtime"`
}
type Storage struct {
	Path        string `json:"path"`
	DirectoryID string `json:"directoryId"`
}
type Participant struct {
	ServiceIDs          []string          `json:"serviceIds"`
	HostID              string            `json:"hostId"`
	HostIP              string            `json:"hostIp"`
	Role                string            `json:"role"`
	NodeID              int               `json:"nodeId"`
	Unit                string            `json:"unit"`
	ConfigPath          string            `json:"configPath"`
	ConfigSHA256        string            `json:"configSha256"`
	UnitSHA256          string            `json:"unitSha256"`
	ArtifactProofSHA256 string            `json:"artifactProofSha256"`
	RuntimeHome         string            `json:"runtimeHome"`
	JavaAgents          map[string]string `json:"javaAgents"`
	ResolvedPaths       map[string]string `json:"resolvedPaths"`
	Storage             []Storage         `json:"storage"`
	PID                 int               `json:"pid"`
	StartTicks          string            `json:"startTicks"`
	ControllerPort      int               `json:"controllerPort"`
}
type Voter struct {
	NodeID      int    `json:"nodeId"`
	DirectoryID string `json:"directoryId"`
	HostIP      string `json:"hostIp"`
	Port        int    `json:"port"`
}
type Plan struct {
	SchemaVersion           string           `json:"schemaVersion"`
	OperationID             string           `json:"operationId"`
	ClusterID               string           `json:"clusterId"`
	ContextHash             string           `json:"contextHash"`
	CandidateHostID         string           `json:"candidateHostId"`
	CandidateIP             string           `json:"candidateIp"`
	KafkaClusterID          string           `json:"kafkaClusterId"`
	KafkaVersion            string           `json:"kafkaVersion"`
	QuorumMode              string           `json:"quorumMode"`
	Layout                  string           `json:"layout"`
	Features                map[string]int16 `json:"features"`
	ArtifactID              string           `json:"artifactId"`
	ArtifactSHA256          string           `json:"artifactSha256"`
	ParcelDir               string           `json:"parcelDir"`
	JavaHome                string           `json:"javaHome"`
	RuntimeUser             string           `json:"runtimeUser"`
	Processes               []Process        `json:"processes"`
	Participants            []Participant    `json:"participants"`
	Voters                  []Voter          `json:"voters"`
	ExcludedHostIDs         []string         `json:"excludedHostIds"`
	ExcludedHostIPs         []string         `json:"excludedHostIps"`
	MaintenanceAcknowledged bool             `json:"maintenanceAcknowledged"`
	ResourcePolicy          ResourcePolicy   `json:"resourcePolicy"`
}

var uuid = regexp.MustCompile(`^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$`)
var hash = regexp.MustCompile(`^[a-f0-9]{64}$`)
var kafkaID = regexp.MustCompile(`^[A-Za-z0-9_-]{22}$`)
var unit = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_@.-]{0,180}\.service$`)
var user = regexp.MustCompile(`^[a-z_][a-z0-9_-]*[$]?$`)
var heap = regexp.MustCompile(`^[1-9][0-9]*[mMgG]$`)
var javaMajor = regexp.MustCompile(`(?:version\s+"?|openjdk\s+)([0-9]+)`)
var propertyKey = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9._-]{0,127}$`)
var protectedProperties = map[string]bool{"process.roles": true, "node.id": true, "broker.id": true, "listeners": true, "advertised.listeners": true, "log.dir": true, "log.dirs": true, "metadata.log.dir": true, "controller.quorum.voters": true, "controller.quorum.bootstrap.servers": true, "controller.quorum.auto.join.enable": true, "controller.listener.names": true, "listener.security.protocol.map": true, "inter.broker.listener.name": true, "broker.rack": true, "zookeeper.connect": true}

func Decode(raw, digest, hostID string) (Plan, error) {
	var p Plan
	if len(raw) > 2*1024*1024 || Digest(raw) != digest || json.Unmarshal([]byte(raw), &p) != nil {
		return p, fmt.Errorf("invalid frozen expansion payload")
	}
	if err := p.Validate(); err != nil {
		return p, err
	}
	if hostID == p.CandidateHostID {
		return p, nil
	}
	for _, member := range p.Participants {
		if member.HostID == hostID {
			return p, nil
		}
	}
	return p, fmt.Errorf("agent host is outside reviewed expansion scope")
}
func Digest(raw string) string {
	value := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(value[:])
}
func (p Plan) Validate() error {
	policy := p.ResourcePolicy
	if policy.MinAvailableMemoryMB < 1 || policy.MinAvailableMemoryMB > 1048576 || policy.OSReserveMB < 1 || policy.OSReserveMB > 1048576 || policy.MinDiskGB < 1 || policy.MinDiskGB > 1048576 || policy.MinInodes < 1 || policy.MinInodes > 1000000000 || len(policy.AllowedJavaMajors) == 0 || len(policy.AllowedJavaMajors) > 8 {
		return fmt.Errorf("bounded reviewed resource policy required")
	}
	for _, major := range policy.AllowedJavaMajors {
		if major < 17 || major > 99 {
			return fmt.Errorf("unsupported Java admission policy")
		}
	}
	if p.SchemaVersion != "add-quorum-expansion/v1" || !uuid.MatchString(p.OperationID) || !uuid.MatchString(p.ClusterID) || !hash.MatchString(p.ContextHash) || p.CandidateHostID == "" || net.ParseIP(p.CandidateIP) == nil || !kafkaID.MatchString(p.KafkaClusterID) || !uuid.MatchString(p.ArtifactID) || !hash.MatchString(p.ArtifactSHA256) || !user.MatchString(p.RuntimeUser) {
		return fmt.Errorf("incomplete expansion identity or artifact")
	}
	if p.KafkaVersion != "4.3.0" && p.KafkaVersion != "4.1.0" && p.KafkaVersion != "4.1.1" && p.KafkaVersion != "4.1.2" {
		return fmt.Errorf("unsupported expansion Kafka adapter")
	}
	if p.QuorumMode != "STATIC" && p.QuorumMode != "DYNAMIC" {
		return fmt.Errorf("observed quorum mode is required")
	}
	if p.Layout != "CONTROLLER" && p.Layout != "SEPARATE" {
		return fmt.Errorf("unsupported candidate process layout")
	}
	if p.Features["metadata.version"] < 1 || (p.QuorumMode == "DYNAMIC" && p.Features["kraft.version"] < 1) || (p.QuorumMode == "STATIC" && p.Features["kraft.version"] != 0) {
		return fmt.Errorf("quorum mode differs from observed finalized features")
	}
	if p.QuorumMode == "STATIC" && !p.MaintenanceAcknowledged {
		return fmt.Errorf("static expansion requires explicit reviewed maintenance acknowledgement")
	}
	if !safePath(p.ParcelDir) || !safePath(p.JavaHome) || p.excluded(p.CandidateHostID, p.CandidateIP) {
		return fmt.Errorf("invalid or excluded candidate runtime")
	}
	if len(p.Participants) == 0 || len(p.Participants) > 128 || len(p.Voters) == 0 || len(p.Voters) > 128 {
		return fmt.Errorf("bounded reviewed participant/voter scope required")
	}
	used := map[int]bool{}
	byID := map[int]Participant{}
	units := map[string]bool{}
	services := map[string]bool{}
	hosts := map[string]string{}
	storageOwners := map[string]bool{}
	directoryOwners := map[string]bool{}
	maxID, maxBroker := 100, 0
	hasBroker := false
	for _, m := range p.Participants {
		if m.HostID == "" || m.HostID == p.CandidateHostID || net.ParseIP(m.HostIP) == nil || net.ParseIP(m.HostIP).Equal(net.ParseIP(p.CandidateIP)) || p.excluded(m.HostID, m.HostIP) || !unit.MatchString(m.Unit) || strings.Contains(m.Unit, "..") || !safeConfig(m.ConfigPath) || !hash.MatchString(m.ConfigSHA256) || !hash.MatchString(m.UnitSHA256) || !hash.MatchString(m.ArtifactProofSHA256) || !safePath(m.RuntimeHome) || m.NodeID < 0 || m.NodeID > 2147483647 || m.PID < 1 || !regexp.MustCompile(`^[1-9][0-9]*$`).MatchString(m.StartTicks) {
			return fmt.Errorf("unverified participant identity or runtime")
		}
		canonicalIP := net.ParseIP(m.HostIP).String()
		if len(m.JavaAgents) > 8 {
			return fmt.Errorf("bounded participant Java agent approvals required")
		}
		for file, checksum := range m.JavaAgents {
			if !safePath(file) || !hash.MatchString(checksum) {
				return fmt.Errorf("invalid participant Java agent approval")
			}
		}
		if len(m.ResolvedPaths) > 32 {
			return fmt.Errorf("too many participant path bindings")
		}
		for logical, resolved := range m.ResolvedPaths {
			if !participantBinding(m, logical, resolved) {
				return fmt.Errorf("path binding is outside participant configuration, unit or storage")
			}
		}
		if owner, exists := hosts[canonicalIP]; exists && owner != m.HostID {
			return fmt.Errorf("participant host aliases are ambiguous")
		}
		hosts[canonicalIP] = m.HostID
		if m.Role != "broker" && m.Role != "controller" && m.Role != "broker,controller" {
			return fmt.Errorf("unsupported participant role")
		}
		if units[m.HostID+":"+m.Unit] || used[m.NodeID] {
			return fmt.Errorf("participant units and Kafka IDs must be coalesced and unique")
		}
		units[m.HostID+":"+m.Unit] = true
		used[m.NodeID] = true
		byID[m.NodeID] = m
		if len(m.ServiceIDs) == 0 || len(m.ServiceIDs) > 2 {
			return fmt.Errorf("participant service assignment ownership required")
		}
		for _, id := range m.ServiceIDs {
			if !uuid.MatchString(id) || services[id] {
				return fmt.Errorf("duplicate or invalid service ownership")
			}
			services[id] = true
		}
		if len(m.Storage) == 0 || len(m.Storage) > 17 {
			return fmt.Errorf("complete participant storage identities required")
		}
		seenStorage := map[string]bool{}
		seenDirectories := map[string]bool{}
		for _, s := range m.Storage {
			if !safePath(s.Path) || !kafkaID.MatchString(s.DirectoryID) || s.DirectoryID == "AAAAAAAAAAAAAAAAAAAAAA" || seenStorage[s.Path] || seenDirectories[s.DirectoryID] || storageOwners[m.HostID+":"+s.Path] || directoryOwners[s.DirectoryID] {
				return fmt.Errorf("ambiguous participant storage")
			}
			seenStorage[s.Path] = true
			seenDirectories[s.DirectoryID] = true
			storageOwners[m.HostID+":"+s.Path] = true
			directoryOwners[s.DirectoryID] = true
		}
		if m.NodeID > maxID {
			maxID = m.NodeID
		}
		if strings.Contains(m.Role, "broker") {
			hasBroker = true
			if m.NodeID > maxBroker {
				maxBroker = m.NodeID
			}
		}
		if strings.Contains(m.Role, "controller") && !port(m.ControllerPort) {
			return fmt.Errorf("observed controller listener required")
		}
	}
	if !hasBroker {
		return fmt.Errorf("reviewed existing broker membership is required")
	}
	voters := map[int]bool{}
	endpoints := map[string]bool{}
	for _, v := range p.Voters {
		m, exists := byID[v.NodeID]
		endpoint := net.JoinHostPort(v.HostIP, strconv.Itoa(v.Port))
		if !exists || !strings.Contains(m.Role, "controller") || m.HostIP != v.HostIP || m.ControllerPort != v.Port || !kafkaID.MatchString(v.DirectoryID) || voters[v.NodeID] || endpoints[endpoint] {
			return fmt.Errorf("voter identity differs from reviewed participants")
		}
		voters[v.NodeID] = true
		endpoints[endpoint] = true
		if p.QuorumMode == "DYNAMIC" {
			if v.DirectoryID == "AAAAAAAAAAAAAAAAAAAAAA" {
				return fmt.Errorf("dynamic voter directory identity is unavailable")
			}
			found := false
			for _, s := range m.Storage {
				found = found || s.DirectoryID == v.DirectoryID
			}
			if !found {
				return fmt.Errorf("dynamic voter directory is outside observed native storage")
			}
		}
	}
	for _, m := range p.Participants {
		if strings.Contains(m.Role, "controller") && !voters[m.NodeID] {
			return fmt.Errorf("unexplained nonvoting controller participant")
		}
	}
	want := 1
	if p.Layout == "SEPARATE" {
		want = 2
	}
	if len(p.Processes) != want {
		return fmt.Errorf("layout requires exact candidate process cardinality")
	}
	ports := map[int]bool{}
	storage := []string{}
	logs := []string{}
	for i, c := range p.Processes {
		role := "controller"
		nextID := maxID + 1
		if i == 1 {
			role = "broker"
			nextID = maxBroker + 1
		}
		if c.Role != role || c.NodeID != nextID || c.NodeID < 1 || c.NodeID > 2147483647 || used[c.NodeID] || !port(c.Port) || ports[c.Port] || !heap.MatchString(c.HeapXmx) || c.Runtime.LimitNofile < 1024 {
			return fmt.Errorf("invalid sequential candidate identity, port or runtime")
		}
		if _, err := HeapBytes(c.HeapXmx); err != nil {
			return err
		}
		used[c.NodeID] = true
		ports[c.Port] = true
		if c.Runtime.JmxPort != 0 {
			if !port(c.Runtime.JmxPort) || ports[c.Runtime.JmxPort] || !uuid.MatchString(c.Runtime.JmxArtifactID) || !hash.MatchString(c.Runtime.JmxChecksum) {
				return fmt.Errorf("distinct approved JMX exporter required")
			}
			ports[c.Runtime.JmxPort] = true
		}
		if c.Runtime.JmxPort == 0 && (c.Runtime.JmxArtifactID != "" || c.Runtime.JmxChecksum != "") {
			return fmt.Errorf("disabled JMX cannot carry an exporter artifact")
		}
		if c.Role == "controller" && c.BrokerRack != "" || !regexp.MustCompile(`^[A-Za-z0-9_.-]{0,128}$`).MatchString(c.BrokerRack) {
			return fmt.Errorf("invalid candidate broker rack")
		}
		if len(c.DataPaths) == 0 || len(c.DataPaths) > 16 || !safePath(c.LogPath) || c.Role == "controller" && c.MetadataPath == "" {
			return fmt.Errorf("explicit isolated process storage and logs required")
		}
		own := map[string]bool{}
		for _, d := range c.DataPaths {
			if !safePath(d) || own[d] {
				return fmt.Errorf("duplicate candidate storage")
			}
			own[d] = true
		}
		if c.MetadataPath != "" {
			if !safePath(c.MetadataPath) {
				return fmt.Errorf("invalid metadata path")
			}
			own[c.MetadataPath] = true
		}
		for d := range own {
			storage = append(storage, d)
		}
		logs = append(logs, c.LogPath)
		for key, value := range c.Inherited {
			lower := strings.ToLower(key)
			if !propertyKey.MatchString(key) || protectedProperties[key] || lower == "cluster.id" || strings.ContainsAny(value, "\r\n\x00\\") || strings.TrimSpace(value) != value || strings.Contains(lower, "password") || strings.Contains(lower, "jaas") || strings.Contains(lower, "secret") || strings.Contains(lower, "credential") || strings.Contains(lower, "token") || strings.Contains(lower, "keystore") || strings.Contains(lower, "truststore") || strings.Contains(lower, "sasl") || strings.Contains(lower, "ssl") || strings.Contains(lower, "authorizer") || lower == "super.users" || lower == "security.protocol" {
				return fmt.Errorf("unsupported inherited property adapter")
			}
		}
	}
	for i, a := range storage {
		for _, b := range storage[i+1:] {
			if overlap(a, b) {
				return fmt.Errorf("candidate storage trees overlap")
			}
		}
		for _, b := range append(append([]string{}, logs...), p.ParcelDir, p.JavaHome) {
			if overlap(a, b) {
				return fmt.Errorf("candidate storage overlaps runtime or logs")
			}
		}
	}
	for i, a := range logs {
		for _, b := range logs[i+1:] {
			if overlap(a, b) {
				return fmt.Errorf("candidate application logs overlap")
			}
		}
		if overlap(a, p.ParcelDir) || overlap(a, p.JavaHome) {
			return fmt.Errorf("candidate logs overlap immutable runtime")
		}
	}
	return nil
}
func (p Plan) excluded(host, ip string) bool {
	for _, h := range p.ExcludedHostIDs {
		if h == host {
			return true
		}
	}
	for _, a := range p.ExcludedHostIPs {
		if net.ParseIP(a) != nil && net.ParseIP(a).Equal(net.ParseIP(ip)) {
			return true
		}
	}
	return false
}
func safePath(p string) bool {
	v, err := taskvalidate.ApprovedPath(p)
	return err == nil && v == p && strings.HasPrefix(p, "/") && path.Clean(p) == p && !strings.ContainsAny(p, " \t\r\n\x00\\") && !strings.HasSuffix(p, "/")
}
func safeConfig(p string) bool {
	if path.Clean(p) != p || !strings.HasPrefix(p, "/") || strings.ContainsAny(p, " \t\r\n\x00\\") {
		return false
	}
	if !safePath(p) && !strings.HasPrefix(p, "/etc/kafka/") && !strings.HasPrefix(p, "/etc/tantor-kafka/") && !strings.HasPrefix(p, "/etc/confluent/") {
		return false
	}
	switch path.Base(p) {
	case "server.properties", "broker.properties", "controller.properties":
		return true
	}
	return false
}
func unitFilePath(name, unitName string) bool {
	return path.Clean(name) == name && path.Base(name) == unitName && (strings.HasPrefix(name, "/etc/systemd/system/") || strings.HasPrefix(name, "/usr/lib/systemd/system/") || strings.HasPrefix(name, "/lib/systemd/system/"))
}
func participantBinding(m Participant, logical, resolved string) bool {
	if logical == m.ConfigPath {
		return safeConfig(resolved) && path.Base(logical) == path.Base(resolved)
	}
	if unitFilePath(logical, m.Unit) {
		return unitFilePath(resolved, m.Unit)
	}
	for _, storage := range m.Storage {
		if logical == path.Join(storage.Path, "meta.properties") {
			return path.Base(resolved) == "meta.properties" && safePath(path.Dir(resolved))
		}
	}
	return false
}
func port(p int) bool { return p > 0 && p < 65536 }
func HeapBytes(value string) (uint64, error) {
	if !heap.MatchString(value) {
		return 0, fmt.Errorf("explicit bounded heap required")
	}
	quantity, err := strconv.ParseUint(value[:len(value)-1], 10, 64)
	factor := uint64(1024 * 1024)
	if strings.EqualFold(value[len(value)-1:], "g") {
		factor *= 1024
	}
	if err != nil || quantity > math.MaxInt64/factor {
		return 0, fmt.Errorf("heap exceeds native memory accounting bounds")
	}
	return quantity * factor, nil
}
func overlap(a, b string) bool {
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}
func (p Plan) Process(role string) (Process, error) {
	for _, c := range p.Processes {
		if c.Role == role {
			return c, nil
		}
	}
	return Process{}, fmt.Errorf("process is outside frozen layout")
}
func (p Plan) ConfigPath(role string) string {
	return "/etc/tantor-kafka/add-expansion/" + p.OperationID + "/" + role + ".properties"
}
func (p Plan) Unit(role string) string {
	c, err := p.Process(role)
	if err != nil {
		return ""
	}
	return "tantor-kafka-" + role + "-" + strconv.Itoa(c.NodeID) + ".service"
}
func (p Plan) ExistingControllers() []string {
	out := []string{}
	for _, v := range p.Voters {
		out = append(out, net.JoinHostPort(v.HostIP, strconv.Itoa(v.Port)))
	}
	sort.Strings(out)
	return out
}
func (p Plan) ExpandedVoters() string {
	out := []string{}
	for _, v := range p.Voters {
		out = append(out, strconv.Itoa(v.NodeID)+"@"+net.JoinHostPort(v.HostIP, strconv.Itoa(v.Port)))
	}
	c, err := p.Process("controller")
	if err == nil {
		out = append(out, strconv.Itoa(c.NodeID)+"@"+net.JoinHostPort(p.CandidateIP, strconv.Itoa(c.Port)))
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}
func (p Plan) VoterDigest() string {
	rows := []string{}
	for _, v := range p.Voters {
		rows = append(rows, strconv.Itoa(v.NodeID)+":"+v.DirectoryID+"@"+net.JoinHostPort(v.HostIP, strconv.Itoa(v.Port)))
	}
	sort.Strings(rows)
	return Digest(strings.Join(rows, ","))
}
func (p Plan) Properties(role string) (string, error) {
	if err := p.Validate(); err != nil {
		return "", err
	}
	c, err := p.Process(role)
	if err != nil {
		return "", err
	}
	values := map[string]string{}
	for key, value := range c.Inherited {
		values[key] = value
	}
	values["process.roles"] = role
	values["node.id"] = strconv.Itoa(c.NodeID)
	listener := "CONTROLLER"
	if role == "broker" {
		listener = "PLAINTEXT"
	}
	values["listeners"] = listener + "://" + net.JoinHostPort(p.CandidateIP, strconv.Itoa(c.Port))
	if role == "broker" {
		values["advertised.listeners"] = values["listeners"]
	}
	values["controller.listener.names"] = "CONTROLLER"
	values["listener.security.protocol.map"] = "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"
	values["inter.broker.listener.name"] = "PLAINTEXT"
	values["log.dirs"] = strings.Join(c.DataPaths, ",")
	if c.MetadataPath != "" {
		values["metadata.log.dir"] = c.MetadataPath
	}
	if c.BrokerRack != "" {
		values["broker.rack"] = c.BrokerRack
	}
	if p.QuorumMode == "STATIC" {
		values["controller.quorum.voters"] = p.ExpandedVoters()
	} else {
		values["controller.quorum.bootstrap.servers"] = strings.Join(p.ExistingControllers(), ",")
	}
	keys := []string{}
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out strings.Builder
	for _, key := range keys {
		out.WriteString(key + "=" + values[key] + "\n")
	}
	return out.String(), nil
}
