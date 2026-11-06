package expansion

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ParticipantReceipt struct {
	SchemaVersion  string `json:"schemaVersion"`
	OperationID    string `json:"operationId"`
	PlanDigest     string `json:"planDigest"`
	HostID         string `json:"hostId"`
	Unit           string `json:"unit"`
	NodeID         int    `json:"nodeId"`
	OriginalSHA256 string `json:"originalSha256"`
	ExpandedSHA256 string `json:"expandedSha256"`
	Phase          string `json:"phase"`
}
type ParticipantObservation struct {
	ConfigSHA256    string `json:"configSha256"`
	UnitSHA256      string `json:"unitSha256"`
	StorageVerified bool   `json:"storageVerified"`
	RuntimeVerified bool   `json:"runtimeVerified"`
	PID             int    `json:"pid"`
	StartTicks      string `json:"startTicks"`
	Active          bool   `json:"active"`
	Stopped         bool   `json:"stopped"`
	BackupVerified  bool   `json:"backupVerified"`
	StageVerified   bool   `json:"stageVerified"`
}
type ParticipantReport struct {
	Receipt     ParticipantReceipt     `json:"receipt"`
	Observation ParticipantObservation `json:"observation"`
	ObservedAt  time.Time              `json:"observedAt"`
}
type ParticipantEnvironment interface {
	LoadParticipantReceipt(Participant) (*ParticipantReceipt, error)
	SaveParticipantReceipt(ParticipantReceipt) error
	ObserveParticipant(context.Context, Plan, Participant) (ParticipantObservation, error)
	ReadOriginal(context.Context, Participant) (string, error)
	Stage(context.Context, Participant, string, string) error
	StopParticipant(context.Context, Participant) error
	ApplyParticipant(context.Context, Participant, string, string) error
	StartParticipant(context.Context, Participant) error
}

// ExpandedConfig preserves every unrelated property and comment. The adapter
// rejects Java Properties escape/continuation syntax rather than guessing its
// meaning while changing a running cluster's voter configuration.
func ExpandedConfig(p Plan, m Participant, original string) (string, error) {
	if p.QuorumMode != "STATIC" || len(original) > 262144 || Digest(original) != m.ConfigSHA256 {
		return "", fmt.Errorf("verified static participant configuration required")
	}
	props := map[string]string{}
	lines := strings.Split(original, "\n")
	votersIndex := -1
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "!") {
			continue
		}
		if strings.ContainsAny(line, "\\\x00") {
			return "", fmt.Errorf("escaped participant properties require an explicit adapter")
		}
		kv := strings.SplitN(trimmed, "=", 2)
		if len(kv) != 2 {
			return "", fmt.Errorf("ambiguous participant property")
		}
		key, value := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
		if !propertyKey.MatchString(key) {
			return "", fmt.Errorf("ambiguous participant property key")
		}
		if _, exists := props[key]; exists {
			return "", fmt.Errorf("duplicate participant property")
		}
		props[key] = value
		if key == "controller.quorum.voters" {
			votersIndex = i
		}
	}
	roles := strings.Split(props["process.roles"], ",")
	for i := range roles {
		roles[i] = strings.TrimSpace(roles[i])
	}
	sort.Strings(roles)
	if props["node.id"] != strconv.Itoa(m.NodeID) || strings.Join(roles, ",") != m.Role || votersIndex < 0 || props["controller.quorum.bootstrap.servers"] != "" {
		return "", fmt.Errorf("participant role, ID or quorum mode changed")
	}
	actual := strings.Split(props["controller.quorum.voters"], ",")
	for i := range actual {
		actual[i] = strings.TrimSpace(actual[i])
	}
	sort.Strings(actual)
	expected := []string{}
	for _, v := range p.Voters {
		expected = append(expected, strconv.Itoa(v.NodeID)+"@"+hostPort(v.HostIP, v.Port))
	}
	sort.Strings(expected)
	if strings.Join(actual, ",") != strings.Join(expected, ",") {
		return "", fmt.Errorf("participant voters differ from frozen baseline")
	}
	storage := map[string]bool{}
	for _, d := range strings.Split(props["log.dirs"], ",") {
		storage[strings.TrimSpace(d)] = true
	}
	if props["metadata.log.dir"] != "" {
		storage[props["metadata.log.dir"]] = true
	}
	if len(storage) != len(m.Storage) {
		return "", fmt.Errorf("participant storage paths changed")
	}
	for _, s := range m.Storage {
		if !storage[s.Path] {
			return "", fmt.Errorf("participant storage paths changed")
		}
	}
	suffix := ""
	if strings.HasSuffix(lines[votersIndex], "\r") {
		suffix = "\r"
	}
	lines[votersIndex] = "controller.quorum.voters=" + p.ExpandedVoters() + suffix
	return strings.Join(lines, "\n"), nil
}
func hostPort(host string, port int) string {
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return host + ":" + strconv.Itoa(port)
}

// Every existing-unit effect is journalled first. No controller/broker storage
// is formatted, deleted or reassigned by this workflow.
func executeParticipant(ctx context.Context, p Plan, digest, hostID, unitName, action string, env ParticipantEnvironment) (ParticipantReport, error) {
	var report ParticipantReport
	if err := ctx.Err(); err != nil {
		return report, err
	}
	if err := p.Validate(); err != nil {
		return report, err
	}
	if !hash.MatchString(digest) || p.QuorumMode != "STATIC" {
		return report, fmt.Errorf("reviewed static expansion required")
	}
	var member *Participant
	for _, m := range p.Participants {
		if m.HostID == hostID && m.Unit == unitName {
			copy := m
			member = &copy
		}
	}
	if member == nil {
		return report, fmt.Errorf("participant is outside frozen host/unit scope")
	}
	m := *member
	if action != "INSPECT" && action != "STAGE_VOTERS" && action != "STOP" && action != "APPLY_VOTERS" && action != "VERIFY_CONFIG" && action != "START" {
		return report, fmt.Errorf("unsupported participant action")
	}
	original, err := env.ReadOriginal(ctx, m)
	if err != nil {
		return report, err
	}
	expanded, err := ExpandedConfig(p, m, original)
	if err != nil {
		return report, err
	}
	r := ParticipantReceipt{SchemaVersion: "add-quorum-participant/v1", OperationID: p.OperationID, PlanDigest: digest, HostID: hostID, Unit: m.Unit, NodeID: m.NodeID, OriginalSHA256: m.ConfigSHA256, ExpandedSHA256: Digest(expanded), Phase: "ABSENT"}
	stored, err := env.LoadParticipantReceipt(m)
	if err != nil {
		return report, err
	}
	if stored != nil {
		if stored.SchemaVersion != r.SchemaVersion || stored.OperationID != r.OperationID || stored.PlanDigest != digest || stored.HostID != hostID || stored.Unit != m.Unit || stored.NodeID != m.NodeID || stored.OriginalSHA256 != r.OriginalSHA256 || stored.ExpandedSHA256 != r.ExpandedSHA256 || !participantPhase(stored.Phase) {
			return report, fmt.Errorf("foreign participant ownership receipt")
		}
		r = *stored
	}
	observe := func() (ParticipantObservation, error) {
		o, e := env.ObserveParticipant(ctx, p, m)
		if e != nil {
			return o, e
		}
		if !o.StorageVerified || o.UnitSHA256 != m.UnitSHA256 || o.ConfigSHA256 != r.OriginalSHA256 && o.ConfigSHA256 != r.ExpandedSHA256 || o.Active && o.Stopped {
			return o, fmt.Errorf("participant files or storage identity changed")
		}
		if o.Active && !o.RuntimeVerified {
			return o, fmt.Errorf("participant active runtime is unverified")
		}
		return o, nil
	}
	finish := func() (ParticipantReport, error) {
		o, e := observe()
		return ParticipantReport{Receipt: r, Observation: o, ObservedAt: time.Now().UTC()}, e
	}
	if action == "INSPECT" {
		return finish()
	}
	if stored == nil && action != "STAGE_VOTERS" {
		return report, fmt.Errorf("participant has no staged ownership receipt")
	}
	if action == "STAGE_VOTERS" {
		if r.Phase != "ABSENT" && r.Phase != "STAGE_INTENT" {
			return finish()
		}
		o, e := observe()
		if e != nil {
			return report, e
		}
		if !o.Active || o.PID != m.PID || o.StartTicks != m.StartTicks || o.ConfigSHA256 != r.OriginalSHA256 {
			return report, fmt.Errorf("reviewed participant process changed before staging")
		}
		r.Phase = "STAGE_INTENT"
		if err = env.SaveParticipantReceipt(r); err != nil {
			return report, err
		}
		if err = env.Stage(ctx, m, original, expanded); err != nil {
			return report, err
		}
		o, e = observe()
		if e != nil {
			return report, e
		}
		if !o.BackupVerified || !o.StageVerified {
			return report, fmt.Errorf("backup/staging verification failed")
		}
		r.Phase = "STAGED"
	} else {
		o, e := observe()
		if e != nil {
			return report, e
		}
		if !o.BackupVerified || !o.StageVerified {
			return report, fmt.Errorf("verified participant backup and staging required")
		}
		switch action {
		case "STOP":
			if r.Phase == "STOPPED" || r.Phase == "APPLY_INTENT" || r.Phase == "CONFIG_APPLIED" || r.Phase == "START_INTENT" || r.Phase == "STARTED" {
				if !o.Stopped {
					return report, fmt.Errorf("participant is no longer stopped")
				}
				return finish()
			}
			if r.Phase != "STAGED" && r.Phase != "STOP_INTENT" || o.ConfigSHA256 != r.OriginalSHA256 {
				return report, fmt.Errorf("participant stop checkpoint is invalid")
			}
			if o.Active && (o.PID != m.PID || o.StartTicks != m.StartTicks) {
				return report, fmt.Errorf("a different participant process appeared before stop")
			}
			if !o.Stopped {
				if !o.Active {
					return report, fmt.Errorf("participant lifecycle outcome is ambiguous")
				}
				r.Phase = "STOP_INTENT"
				if err = env.SaveParticipantReceipt(r); err != nil {
					return report, err
				}
				if err = env.StopParticipant(ctx, m); err != nil {
					return report, fmt.Errorf("stop outcome uncertain: %w", err)
				}
				o, e = observe()
				if e != nil {
					return report, e
				}
				if !o.Stopped {
					return report, fmt.Errorf("participant stop is unconfirmed")
				}
			}
			r.Phase = "STOPPED"
		case "APPLY_VOTERS":
			if !o.Stopped || r.Phase != "STOPPED" && r.Phase != "APPLY_INTENT" && r.Phase != "CONFIG_APPLIED" {
				return report, fmt.Errorf("participant must be durably stopped before configuration apply")
			}
			if o.ConfigSHA256 == r.OriginalSHA256 {
				r.Phase = "APPLY_INTENT"
				if err = env.SaveParticipantReceipt(r); err != nil {
					return report, err
				}
				if err = env.ApplyParticipant(ctx, m, original, expanded); err != nil {
					return report, fmt.Errorf("configuration apply outcome uncertain: %w", err)
				}
			}
			o, e = observe()
			if e != nil {
				return report, e
			}
			if !o.Stopped || o.ConfigSHA256 != r.ExpandedSHA256 {
				return report, fmt.Errorf("expanded configuration is unconfirmed")
			}
			r.Phase = "CONFIG_APPLIED"
		case "VERIFY_CONFIG":
			if o.ConfigSHA256 != r.ExpandedSHA256 {
				return report, fmt.Errorf("expanded voter configuration differs")
			}
			return finish()
		case "START":
			if r.Phase != "CONFIG_APPLIED" && r.Phase != "START_INTENT" && r.Phase != "STARTED" || o.ConfigSHA256 != r.ExpandedSHA256 {
				return report, fmt.Errorf("participant expanded configuration checkpoint required")
			}
			if !o.Active {
				if !o.Stopped {
					return report, fmt.Errorf("participant lifecycle outcome is ambiguous")
				}
				r.Phase = "START_INTENT"
				if err = env.SaveParticipantReceipt(r); err != nil {
					return report, err
				}
				if err = env.StartParticipant(ctx, m); err != nil {
					return report, fmt.Errorf("start outcome uncertain: %w", err)
				}
				o, e = observe()
				if e != nil {
					return report, e
				}
				if !o.Active || !o.RuntimeVerified {
					return report, fmt.Errorf("restarted participant identity is unconfirmed")
				}
			}
			r.Phase = "STARTED"
		}
	}
	if err = env.SaveParticipantReceipt(r); err != nil {
		return report, err
	}
	return finish()
}
func participantPhase(phase string) bool {
	switch phase {
	case "STAGE_INTENT", "STAGED", "STOP_INTENT", "STOPPED", "APPLY_INTENT", "CONFIG_APPLIED", "START_INTENT", "STARTED":
		return true
	}
	return false
}
