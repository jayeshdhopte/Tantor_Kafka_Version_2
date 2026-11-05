package readiness

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"io.translab/tantor-agent/internal/taskvalidate"
)

// BrokerPlan is deliberately distinct from the legacy INSTALL_KAFKA envelope.
// It cannot request a controller, a new cluster identity or arbitrary commands.
type BrokerPlan struct {
	SchemaVersion       string   `json:"schemaVersion"`
	OperationID         string   `json:"operationId"`
	HostID              string   `json:"hostId"`
	ContextHash         string   `json:"contextHash"`
	KafkaClusterID      string   `json:"kafkaClusterId"`
	NodeID              int      `json:"nodeId"`
	HostIP              string   `json:"hostIp"`
	BrokerPort          int      `json:"brokerPort"`
	KafkaVersion        string   `json:"kafkaVersion"`
	RuntimeUser         string   `json:"runtimeUser"`
	JavaHome            string   `json:"javaHome"`
	ParcelDir           string   `json:"parcelDir"`
	ArtifactChecksum    string   `json:"artifactChecksum"`
	DataPaths           []string `json:"dataPaths"`
	MetadataPath        string   `json:"metadataPath"`
	LogPath             string   `json:"logPath"`
	InstallPath         string   `json:"installPath"`
	HeapXmx             string   `json:"heapXmx"`
	QuorumMode          string   `json:"quorumMode"`
	ControllerEndpoints []string `json:"controllerEndpoints"`
	QuorumVoters        string   `json:"quorumVoters"`
	BrokerRack          string   `json:"brokerRack"`
}

func ValidateBrokerPlan(raw, hostID string) (BrokerPlan, error) {
	var p BrokerPlan
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return p, fmt.Errorf("invalid broker plan")
	}
	hex := regexp.MustCompile(`^[a-f0-9]{64}$`)
	if p.SchemaVersion != "add-broker/v1" || p.HostID != hostID || !regexp.MustCompile(`^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$`).MatchString(p.OperationID) || !hex.MatchString(p.ContextHash) || !hex.MatchString(strings.ToLower(p.ArtifactChecksum)) || p.NodeID < 1 || p.BrokerPort < 1 || p.BrokerPort > 65535 || net.ParseIP(p.HostIP) == nil || !regexp.MustCompile(`^[A-Za-z0-9_-]{22}$`).MatchString(p.KafkaClusterID) {
		return p, fmt.Errorf("broker identity, operation or artifact mismatch")
	}
	if !versionPattern.MatchString(p.KafkaVersion) || !userPattern.MatchString(p.RuntimeUser) || !regexp.MustCompile(`^[1-9][0-9]*[mMgG]$`).MatchString(p.HeapXmx) {
		return p, fmt.Errorf("explicit version, runtime user and heap required")
	}
	if !regexp.MustCompile(`^[A-Za-z0-9_.-]{0,128}$`).MatchString(p.BrokerRack) {
		return p, fmt.Errorf("invalid candidate broker rack")
	}
	if len(p.DataPaths) == 0 || len(p.DataPaths) > 16 || len(p.ControllerEndpoints) == 0 || len(p.ControllerEndpoints) > 128 {
		return p, fmt.Errorf("explicit bounded storage and controller discovery required")
	}
	paths := append(append([]string{}, p.DataPaths...), p.JavaHome, p.ParcelDir, p.LogPath, p.InstallPath)
	if p.MetadataPath != "" {
		paths = append(paths, p.MetadataPath)
	}
	for _, v := range paths {
		if v == "" || strings.ContainsAny(v, " \t%\\") || path.Clean(v) != v {
			return p, fmt.Errorf("non-canonical broker path")
		}
		if _, err := taskvalidate.ApprovedPath(v); err != nil {
			return p, err
		}
	}
	for _, ep := range p.ControllerEndpoints {
		if _, err := taskvalidate.HostPort(ep); err != nil {
			return p, err
		}
	}
	if p.QuorumMode != "STATIC" && p.QuorumMode != "DYNAMIC" {
		return p, fmt.Errorf("unknown quorum mode")
	}
	if p.QuorumMode == "STATIC" {
		if p.QuorumVoters == "" {
			return p, fmt.Errorf("static voters required")
		}
		for _, v := range strings.Split(p.QuorumVoters, ",") {
			parts := strings.Split(v, "@")
			if len(parts) != 2 {
				return p, fmt.Errorf("invalid voter")
			}
			id, err := strconv.Atoi(parts[0])
			if err != nil || id < 0 || id == p.NodeID {
				return p, fmt.Errorf("broker cannot become a voter")
			}
			if _, err = taskvalidate.HostPort(parts[1]); err != nil {
				return p, err
			}
		}
	}
	storage := append([]string{}, p.DataPaths...)
	if p.MetadataPath != "" {
		found := false
		for _, v := range storage {
			found = found || v == p.MetadataPath
		}
		if !found {
			storage = append(storage, p.MetadataPath)
		}
	}
	overlap := func(a, b string) bool { return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/") }
	for i, v := range storage {
		for j := i + 1; j < len(storage); j++ {
			if overlap(v, storage[j]) {
				return p, fmt.Errorf("overlapping storage")
			}
		}
		for _, other := range []string{p.JavaHome, p.ParcelDir, p.LogPath, p.InstallPath} {
			if overlap(v, other) {
				return p, fmt.Errorf("storage overlaps runtime paths")
			}
		}
	}
	return p, nil
}

// ValidateBrokerPayload re-verifies installed executable bytes before any future
// execution command can use the frozen plan. This command itself never mutates.
func ValidateBrokerPayload(ctx context.Context, p BrokerPlan) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	r := Request{InstallPath: p.InstallPath, ParcelDir: p.ParcelDir, KafkaVersion: p.KafkaVersion}
	archive := findArchive(r)
	if archive == "" {
		return fmt.Errorf("approved cached archive unavailable")
	}
	hash, err := hashArchive(ctx, archive)
	if err != nil || !strings.EqualFold(hash, p.ArtifactChecksum) {
		return fmt.Errorf("approved archive checksum mismatch")
	}
	return verifyInstalledArchive(ctx, archive, path.Join(p.ParcelDir, "kafka", p.KafkaVersion))
}

func (p BrokerPlan) Properties() string {
	endpoint := net.JoinHostPort(p.HostIP, strconv.Itoa(p.BrokerPort))
	s := fmt.Sprintf("process.roles=broker\nnode.id=%d\nlisteners=PLAINTEXT://%s\nadvertised.listeners=PLAINTEXT://%s\nlistener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT\ninter.broker.listener.name=PLAINTEXT\ncontroller.listener.names=CONTROLLER\nlog.dirs=%s\n", p.NodeID, endpoint, endpoint, strings.Join(p.DataPaths, ","))
	if p.MetadataPath != "" {
		s += "metadata.log.dir=" + p.MetadataPath + "\n"
	}
	if p.BrokerRack != "" {
		s += "broker.rack=" + p.BrokerRack + "\n"
	}
	if p.QuorumMode == "DYNAMIC" {
		s += "controller.quorum.bootstrap.servers=" + strings.Join(p.ControllerEndpoints, ",") + "\n"
	} else {
		s += "controller.quorum.voters=" + p.QuorumVoters + "\n"
	}
	return s
}
