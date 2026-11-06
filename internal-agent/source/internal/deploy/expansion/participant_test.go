package expansion

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type participantEnv struct {
	receipt                *ParticipantReceipt
	original, expanded     string
	observation            ParticipantObservation
	stops, applies, starts int
	fail                   string
}

func (e *participantEnv) LoadParticipantReceipt(Participant) (*ParticipantReceipt, error) {
	if e.receipt == nil {
		return nil, nil
	}
	copy := *e.receipt
	return &copy, nil
}
func (e *participantEnv) SaveParticipantReceipt(r ParticipantReceipt) error {
	copy := r
	e.receipt = &copy
	return nil
}
func (e *participantEnv) ObserveParticipant(context.Context, Plan, Participant) (ParticipantObservation, error) {
	return e.observation, nil
}
func (e *participantEnv) ReadOriginal(context.Context, Participant) (string, error) {
	return e.original, nil
}
func (e *participantEnv) Stage(_ context.Context, _ Participant, original, expanded string) error {
	e.expanded = expanded
	e.observation.BackupVerified = true
	e.observation.StageVerified = true
	if e.fail == "stage" {
		return errors.New("lost stage result")
	}
	return nil
}
func (e *participantEnv) StopParticipant(context.Context, Participant) error {
	e.stops++
	e.observation.Active = false
	e.observation.Stopped = true
	if e.fail == "stop" {
		return errors.New("lost stop result")
	}
	return nil
}
func (e *participantEnv) ApplyParticipant(context.Context, Participant, string, string) error {
	e.applies++
	e.observation.ConfigSHA256 = Digest(e.expanded)
	if e.fail == "apply" {
		return errors.New("lost apply result")
	}
	return nil
}
func (e *participantEnv) StartParticipant(context.Context, Participant) error {
	e.starts++
	e.observation.Active = true
	e.observation.Stopped = false
	e.observation.PID++
	e.observation.StartTicks = "987654"
	if e.fail == "start" {
		return errors.New("lost start result")
	}
	return nil
}
func participantFixture() (Plan, Participant, *participantEnv) {
	p := fixture()
	p.QuorumMode = "STATIC"
	p.Features["kraft.version"] = 0
	p.MaintenanceAcknowledged = true
	m := p.Participants[0]
	original := "# preserved comment\nprocess.roles=broker\nnode.id=1\nlog.dirs=/data/broker\ncontroller.quorum.voters=101@192.0.2.2:9093\nnum.network.threads=3\n"
	m.ConfigSHA256 = Digest(original)
	p.Participants[0] = m
	e := &participantEnv{original: original, observation: ParticipantObservation{ConfigSHA256: m.ConfigSHA256, UnitSHA256: m.UnitSHA256, StorageVerified: true, RuntimeVerified: true, PID: m.PID, StartTicks: m.StartTicks, Active: true}}
	return p, m, e
}
func participantAction(p Plan, m Participant, e *participantEnv, action string) (ParticipantReport, error) {
	return executeParticipant(context.Background(), p, strings.Repeat("a", 64), m.HostID, m.Unit, action, e)
}
func TestStaticParticipantRecoveryReconcilesEveryLostEffectWithoutRepeatingIt(t *testing.T) {
	for _, failure := range []string{"stage", "stop", "apply", "start"} {
		p, m, e := participantFixture()
		for _, step := range []string{"STAGE_VOTERS", "STOP", "APPLY_VOTERS", "VERIFY_CONFIG", "START"} {
			matches := map[string]string{"STAGE_VOTERS": "stage", "STOP": "stop", "APPLY_VOTERS": "apply", "START": "start"}
			if matches[step] == failure {
				e.fail = failure
				if _, err := participantAction(p, m, e, step); err == nil {
					t.Fatal("failure ignored", step)
				}
				e.fail = ""
			}
			if _, err := participantAction(p, m, e, step); err != nil {
				t.Fatal(failure, step, err)
			}
		}
		if e.stops != 1 || e.applies != 1 || e.starts != 1 || e.receipt.Phase != "STARTED" {
			t.Fatal(failure, e)
		}
	}
}
func TestExpandedConfigChangesOnlyReviewedVotersAndPreservesOtherProperties(t *testing.T) {
	p, m, e := participantFixture()
	expanded, err := ExpandedConfig(p, m, e.original)
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Replace(e.original, "controller.quorum.voters=101@192.0.2.2:9093", "controller.quorum.voters=101@192.0.2.2:9093,102@192.0.2.3:9093", 1)
	if expanded != want {
		t.Fatal(expanded)
	}
}
func TestParticipantConfigRejectsAmbiguousSyntaxIdentityAndMembership(t *testing.T) {
	for _, suffix := range []string{"node.id=2\n", "node\\.id=1\n", "controller.quorum.voters=101@192.0.2.2:9093\n", "property without equals\n", "evil.property=trailing\\\n"} {
		p, m, e := participantFixture()
		e.original += suffix
		m.ConfigSHA256 = Digest(e.original)
		if _, err := ExpandedConfig(p, m, e.original); err == nil {
			t.Fatal("ambiguous syntax accepted", suffix)
		}
	}
	for _, replacement := range []string{"node.id=2", "log.dirs=/data/other", "process.roles=broker,controller", "controller.quorum.voters=101@192.0.2.9:9093"} {
		p, m, e := participantFixture()
		key := strings.SplitN(replacement, "=", 2)[0]
		for _, line := range strings.Split(e.original, "\n") {
			if strings.HasPrefix(line, key+"=") {
				e.original = strings.Replace(e.original, line, replacement, 1)
			}
		}
		m.ConfigSHA256 = Digest(e.original)
		if _, err := ExpandedConfig(p, m, e.original); err == nil {
			t.Fatal("changed identity accepted", replacement)
		}
	}
}
func TestStaticParticipantCannotStopReplacementProcessOrApplyWhileRunning(t *testing.T) {
	p, m, e := participantFixture()
	if _, err := participantAction(p, m, e, "STAGE_VOTERS"); err != nil {
		t.Fatal(err)
	}
	if _, err := participantAction(p, m, e, "APPLY_VOTERS"); err == nil {
		t.Fatal("live config rewritten")
	}
	e.observation.PID++
	if _, err := participantAction(p, m, e, "STOP"); err == nil || e.stops != 0 {
		t.Fatal("replacement process stopped")
	}
}
func TestParticipantRejectsForeignReceiptMissingBackupAndChangedStorage(t *testing.T) {
	for _, change := range []func(*participantEnv){func(e *participantEnv) { e.receipt.PlanDigest = strings.Repeat("b", 64) }, func(e *participantEnv) { e.observation.BackupVerified = false }, func(e *participantEnv) { e.observation.StorageVerified = false }, func(e *participantEnv) { e.observation.UnitSHA256 = strings.Repeat("c", 64) }} {
		p, m, e := participantFixture()
		if _, err := participantAction(p, m, e, "STAGE_VOTERS"); err != nil {
			t.Fatal(err)
		}
		change(e)
		if _, err := participantAction(p, m, e, "STOP"); err == nil || e.stops != 0 {
			t.Fatal("unsafe participant stopped")
		}
	}
}
func TestDynamicModeAndForeignUnitsCannotChangeExistingParticipants(t *testing.T) {
	p, m, e := participantFixture()
	p.QuorumMode = "DYNAMIC"
	p.Features["kraft.version"] = 1
	if _, err := participantAction(p, m, e, "STAGE_VOTERS"); err == nil {
		t.Fatal("dynamic participant mutation accepted")
	}
	p, m, e = participantFixture()
	if _, err := executeParticipant(context.Background(), p, strings.Repeat("a", 64), m.HostID, "different.service", "STOP", e); err == nil {
		t.Fatal("foreign unit accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := executeParticipant(ctx, p, strings.Repeat("a", 64), m.HostID, m.Unit, "STOP", e); err == nil {
		t.Fatal("cancelled mutation accepted")
	}
}
