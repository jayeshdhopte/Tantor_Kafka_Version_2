package expansion

import (
	"context"
	"fmt"
	"reflect"
	"time"
)

type CandidateReceipt struct {
	SchemaVersion        string            `json:"schemaVersion"`
	OperationID          string            `json:"operationId"`
	PlanDigest           string            `json:"planDigest"`
	HostID               string            `json:"hostId"`
	Role                 string            `json:"role"`
	NodeID               int               `json:"nodeId"`
	KafkaClusterID       string            `json:"kafkaClusterId"`
	Unit                 string            `json:"unit"`
	ConfigPath           string            `json:"configPath"`
	Phase                string            `json:"phase"`
	DirectoryIDs         map[string]string `json:"directoryIds"`
	LastAdmissionProofID string            `json:"lastAdmissionProofId"`
	AdmissionIntentAt    time.Time         `json:"admissionIntentAt"`
}
type CandidateObservation struct {
	StorageState        string            `json:"storageState"`
	ConfigOwned         bool              `json:"configOwned"`
	UnitOwned           bool              `json:"unitOwned"`
	ServiceActive       bool              `json:"serviceActive"`
	RuntimeVerified     bool              `json:"runtimeVerified"`
	DirectoryIDs        map[string]string `json:"directoryIds"`
	MetadataDirectoryID string            `json:"metadataDirectoryId"`
}
type CandidateReport struct {
	Receipt     CandidateReceipt     `json:"receipt"`
	Observation CandidateObservation `json:"observation"`
	ObservedAt  time.Time            `json:"observedAt"`
}

// ControllerProof is created by the backend's live typed Kafka observation, never by a user payload.
type ControllerProof struct {
	ObservationID    string    `json:"observationId"`
	OperationID      string    `json:"operationId"`
	PlanDigest       string    `json:"planDigest"`
	NodeID           int       `json:"nodeId"`
	DirectoryID      string    `json:"directoryId"`
	BaselineDigest   string    `json:"baselineDigest"`
	ObservedAt       time.Time `json:"observedAt"`
	ObserverCaughtUp bool      `json:"observerCaughtUp"`
	VoterVerified    bool      `json:"voterVerified"`
}

// CandidateEnvironment has a Linux adapter and a deterministic failure-injection
// adapter. The production caller must hold the operation/host lock throughout.
type CandidateEnvironment interface {
	LoadReceipt(role string) (*CandidateReceipt, error)
	SaveReceipt(CandidateReceipt) error
	ValidateArtifact(context.Context, Plan) error
	ValidateFresh(context.Context, Plan, Process) error
	Observe(context.Context, Plan, Process) (CandidateObservation, error)
	Prepare(context.Context, Plan, Process) error
	Format(context.Context, Plan, Process) error
	Start(context.Context, Plan, Process) error
	Admit(context.Context, Plan, Process) error
}

func executeCandidate(ctx context.Context, p Plan, digest, hostID, role, action string, proof *ControllerProof, env CandidateEnvironment) (CandidateReport, error) {
	var report CandidateReport
	if err := ctx.Err(); err != nil {
		return report, err
	}
	if err := p.Validate(); err != nil {
		return report, err
	}
	if !hash.MatchString(digest) || hostID != p.CandidateHostID {
		return report, fmt.Errorf("candidate task is outside the frozen host scope")
	}
	c, err := p.Process(role)
	if err != nil {
		return report, err
	}
	if action != "INSPECT" && action != "PREPARE" && action != "FORMAT" && action != "START" && action != "ADMIT" {
		return report, fmt.Errorf("unsupported expansion action")
	}
	if action == "ADMIT" && (role != "controller" || p.QuorumMode != "DYNAMIC") {
		return report, fmt.Errorf("only a dynamic controller can be admitted")
	}
	existing, err := env.LoadReceipt(role)
	if err != nil {
		return report, err
	}
	r := CandidateReceipt{SchemaVersion: "add-quorum-candidate/v1", OperationID: p.OperationID, PlanDigest: digest, HostID: hostID, Role: role, NodeID: c.NodeID, KafkaClusterID: p.KafkaClusterID, Unit: p.Unit(role), ConfigPath: p.ConfigPath(role), Phase: "ABSENT", DirectoryIDs: map[string]string{}}
	if existing != nil {
		if existing.SchemaVersion != r.SchemaVersion || existing.OperationID != r.OperationID || existing.PlanDigest != digest || existing.HostID != hostID || existing.Role != role || existing.NodeID != c.NodeID || existing.KafkaClusterID != r.KafkaClusterID || existing.Unit != r.Unit || existing.ConfigPath != r.ConfigPath || !candidatePhase(existing.Phase) {
			return report, fmt.Errorf("foreign or incompatible candidate receipt")
		}
		r = *existing
	}
	observe := func() (CandidateObservation, error) {
		o, e := env.Observe(ctx, p, c)
		if e != nil {
			return o, e
		}
		if e = validateCandidateStorage(p, c, o); e != nil {
			return o, e
		}
		if o.StorageState == "MATCHING" {
			for _, sibling := range p.Processes {
				if sibling.Role == c.Role {
					continue
				}
				receipt, err := env.LoadReceipt(sibling.Role)
				if err != nil {
					return o, err
				}
				if receipt == nil {
					continue
				}
				for _, id := range receipt.DirectoryIDs {
					for _, own := range o.DirectoryIDs {
						if id == own {
							return o, fmt.Errorf("two candidate roles share a formatted directory identity")
						}
					}
				}
			}
		}
		if len(r.DirectoryIDs) > 0 && !reflect.DeepEqual(r.DirectoryIDs, o.DirectoryIDs) {
			return o, fmt.Errorf("candidate directory identity changed after formatting")
		}
		return o, nil
	}
	finish := func() (CandidateReport, error) {
		o, e := observe()
		return CandidateReport{Receipt: r, Observation: o, ObservedAt: time.Now().UTC()}, e
	}
	if action == "INSPECT" {
		return finish()
	}
	if err = env.ValidateArtifact(ctx, p); err != nil {
		return report, err
	}
	if existing == nil {
		if action != "PREPARE" {
			return report, fmt.Errorf("candidate has no preparation ownership receipt")
		}
		if err = env.ValidateFresh(ctx, p, c); err != nil {
			return report, err
		}
		r.Phase = "CLAIMED"
		if err = env.SaveReceipt(r); err != nil {
			return report, err
		}
	}
	switch action {
	case "PREPARE":
		if r.Phase != "CLAIMED" {
			o, e := observe()
			if e != nil {
				return report, e
			}
			if !o.ConfigOwned || !o.UnitOwned {
				return report, fmt.Errorf("prepared candidate ownership changed")
			}
			return finish()
		}
		if err = env.ValidateFresh(ctx, p, c); err != nil {
			return report, err
		}
		if err = env.Prepare(ctx, p, c); err != nil {
			return report, err
		}
		o, e := observe()
		if e != nil {
			return report, e
		}
		if !o.ConfigOwned || !o.UnitOwned || o.StorageState != "MISSING" || o.ServiceActive {
			return report, fmt.Errorf("candidate preparation is not proven")
		}
		r.Phase = "PREPARED"
	case "FORMAT":
		o, e := observe()
		if e != nil {
			return report, e
		}
		if !o.ConfigOwned || !o.UnitOwned {
			return report, fmt.Errorf("owned candidate files must match before formatting")
		}
		if r.Phase == "FORMAT_INTENT" {
			if o.StorageState != "MATCHING" || o.ServiceActive {
				return report, fmt.Errorf("format outcome uncertain; never replay the formatter")
			}
			r.DirectoryIDs = cloneIDs(o.DirectoryIDs)
			r.Phase = "FORMATTED"
			break
		}
		if r.Phase == "FORMATTED" || r.Phase == "START_INTENT" || r.Phase == "STARTED" || r.Phase == "ADMISSION_INTENT" || r.Phase == "ADMISSION_SENT" {
			if o.StorageState != "MATCHING" {
				return report, fmt.Errorf("formatted candidate storage no longer matches")
			}
			return finish()
		}
		if r.Phase != "PREPARED" || o.StorageState != "MISSING" || o.ServiceActive {
			return report, fmt.Errorf("only proven empty prepared storage can be formatted")
		}
		r.Phase = "FORMAT_INTENT"
		if err = env.SaveReceipt(r); err != nil {
			return report, err
		}
		if err = env.Format(ctx, p, c); err != nil {
			return report, fmt.Errorf("formatter result uncertain; preserve intent and inspect storage: %w", err)
		}
		o, e = observe()
		if e != nil {
			return report, e
		}
		if o.StorageState != "MATCHING" || o.ServiceActive || len(o.DirectoryIDs) == 0 {
			return report, fmt.Errorf("formatter did not prove every directory identity")
		}
		r.DirectoryIDs = cloneIDs(o.DirectoryIDs)
		r.Phase = "FORMATTED"
	case "START":
		if r.Phase != "FORMATTED" && r.Phase != "START_INTENT" && r.Phase != "STARTED" && r.Phase != "ADMISSION_INTENT" && r.Phase != "ADMISSION_SENT" {
			return report, fmt.Errorf("formatted durable candidate checkpoint is required")
		}
		o, e := observe()
		if e != nil {
			return report, e
		}
		if !o.ConfigOwned || !o.UnitOwned || o.StorageState != "MATCHING" || len(r.DirectoryIDs) == 0 {
			return report, fmt.Errorf("candidate files and formatted identities must match before start")
		}
		if role == "broker" {
			if err = verifyControllerProof(ctx, p, digest, proof, env, true); err != nil {
				return report, err
			}
		}
		if o.ServiceActive {
			if !o.RuntimeVerified {
				return report, fmt.Errorf("active candidate runtime is not the approved process")
			}
			r.Phase = "STARTED"
			break
		}
		r.Phase = "START_INTENT"
		if err = env.SaveReceipt(r); err != nil {
			return report, err
		}
		if err = env.Start(ctx, p, c); err != nil {
			return report, fmt.Errorf("start outcome uncertain; inspect the owned unit: %w", err)
		}
		o, e = observe()
		if e != nil {
			return report, e
		}
		if !o.ServiceActive || !o.RuntimeVerified || !o.ConfigOwned || !o.UnitOwned || o.StorageState != "MATCHING" {
			return report, fmt.Errorf("started candidate identity is not proven")
		}
		r.Phase = "STARTED"
	case "ADMIT":
		if r.Phase != "STARTED" && r.Phase != "ADMISSION_INTENT" && r.Phase != "ADMISSION_SENT" {
			return report, fmt.Errorf("a running operation-owned observer is required")
		}
		if err = verifyControllerProof(ctx, p, digest, proof, env, false); err != nil {
			return report, err
		}
		if proof.VoterVerified {
			return finish()
		}
		if !proof.ObserverCaughtUp {
			return report, fmt.Errorf("observer catch-up must be proven before admission")
		}
		if r.LastAdmissionProofID != "" && (proof.ObservationID == r.LastAdmissionProofID || !proof.ObservedAt.After(r.AdmissionIntentAt)) {
			return report, fmt.Errorf("uncertain admission requires a new live absence/catch-up observation")
		}
		r.LastAdmissionProofID = proof.ObservationID
		r.AdmissionIntentAt = time.Now().UTC()
		r.Phase = "ADMISSION_INTENT"
		if err = env.SaveReceipt(r); err != nil {
			return report, err
		}
		if err = env.Admit(ctx, p, c); err != nil {
			return report, fmt.Errorf("admission outcome uncertain; reconcile voters before retrying: %w", err)
		}
		r.Phase = "ADMISSION_SENT"
	}
	if err = env.SaveReceipt(r); err != nil {
		return report, err
	}
	return finish()
}
func candidatePhase(phase string) bool {
	switch phase {
	case "CLAIMED", "PREPARED", "FORMAT_INTENT", "FORMATTED", "START_INTENT", "STARTED", "ADMISSION_INTENT", "ADMISSION_SENT":
		return true
	}
	return false
}
func cloneIDs(input map[string]string) map[string]string {
	out := map[string]string{}
	for p, id := range input {
		out[p] = id
	}
	return out
}
func verifyControllerProof(ctx context.Context, p Plan, digest string, proof *ControllerProof, env CandidateEnvironment, requireVoter bool) error {
	c, err := p.Process("controller")
	if err != nil {
		return err
	}
	now := time.Now()
	if proof == nil || !uuid.MatchString(proof.ObservationID) || proof.OperationID != p.OperationID || proof.PlanDigest != digest || proof.NodeID != c.NodeID || !kafkaID.MatchString(proof.DirectoryID) || proof.BaselineDigest != p.VoterDigest() || proof.ObservedAt.Before(now.Add(-60*time.Second)) || proof.ObservedAt.After(now.Add(5*time.Second)) || requireVoter && !proof.VoterVerified {
		return fmt.Errorf("fresh parent-bound controller membership/catch-up proof required")
	}
	o, err := env.Observe(ctx, p, c)
	if err != nil {
		return err
	}
	if err = validateCandidateStorage(p, c, o); err != nil {
		return err
	}
	if !o.ServiceActive || !o.RuntimeVerified || !o.ConfigOwned || !o.UnitOwned || o.StorageState != "MATCHING" || o.MetadataDirectoryID != proof.DirectoryID {
		return fmt.Errorf("controller storage/process differs from live quorum proof")
	}
	return nil
}
func validateCandidateStorage(p Plan, c Process, o CandidateObservation) error {
	if o.StorageState != "MATCHING" {
		return nil
	}
	expected := map[string]bool{}
	for _, path := range c.DataPaths {
		expected[path] = true
	}
	if c.MetadataPath != "" {
		expected[c.MetadataPath] = true
	}
	if len(o.DirectoryIDs) != len(expected) {
		return fmt.Errorf("formatted storage proof omits a candidate directory")
	}
	for _, m := range p.Participants {
		for _, s := range m.Storage {
			for _, id := range o.DirectoryIDs {
				if id == s.DirectoryID {
					return fmt.Errorf("candidate copied an existing participant directory identity")
				}
			}
		}
	}
	seen := map[string]bool{}
	for path, id := range o.DirectoryIDs {
		if !expected[path] || !kafkaID.MatchString(id) || id == "AAAAAAAAAAAAAAAAAAAAAA" || seen[id] {
			return fmt.Errorf("formatted candidate directory identities are ambiguous")
		}
		seen[id] = true
	}
	metadata := c.MetadataPath
	if metadata == "" {
		metadata = c.DataPaths[0]
	}
	if o.MetadataDirectoryID != o.DirectoryIDs[metadata] {
		return fmt.Errorf("metadata directory proof differs from the formatted path")
	}
	return nil
}
