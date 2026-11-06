package expansion

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

type candidateFixture struct {
	receipts                                            map[string]CandidateReceipt
	observations                                        map[string]CandidateObservation
	prepares, formats, starts, admissions               int
	loseFormat, partialFormat, loseStart, loseAdmission bool
}

func newCandidateFixture() *candidateFixture {
	return &candidateFixture{receipts: map[string]CandidateReceipt{}, observations: map[string]CandidateObservation{}}
}
func (f *candidateFixture) LoadReceipt(role string) (*CandidateReceipt, error) {
	r, ok := f.receipts[role]
	if !ok {
		return nil, nil
	}
	r.DirectoryIDs = cloneIDs(r.DirectoryIDs)
	return &r, nil
}
func (f *candidateFixture) SaveReceipt(r CandidateReceipt) error {
	r.DirectoryIDs = cloneIDs(r.DirectoryIDs)
	f.receipts[r.Role] = r
	return nil
}
func (f *candidateFixture) ValidateArtifact(context.Context, Plan) error { return nil }
func (f *candidateFixture) ValidateFresh(_ context.Context, _ Plan, c Process) error {
	o := f.observation(c.Role)
	if o.ServiceActive || o.StorageState != "MISSING" {
		return fmt.Errorf("not fresh")
	}
	return nil
}
func (f *candidateFixture) observation(role string) CandidateObservation {
	o, ok := f.observations[role]
	if !ok {
		return CandidateObservation{StorageState: "MISSING", DirectoryIDs: map[string]string{}}
	}
	o.DirectoryIDs = cloneIDs(o.DirectoryIDs)
	return o
}
func (f *candidateFixture) Observe(_ context.Context, _ Plan, c Process) (CandidateObservation, error) {
	return f.observation(c.Role), nil
}
func (f *candidateFixture) Prepare(_ context.Context, _ Plan, c Process) error {
	f.prepares++
	o := f.observation(c.Role)
	o.ConfigOwned = true
	o.UnitOwned = true
	f.observations[c.Role] = o
	return nil
}
func (f *candidateFixture) Format(_ context.Context, _ Plan, c Process) error {
	f.formats++
	o := f.observation(c.Role)
	o.StorageState = "MATCHING"
	o.DirectoryIDs = map[string]string{}
	for i, p := range c.DataPaths {
		base := 'j'
		if c.Role == "broker" {
			base = 'm'
		}
		o.DirectoryIDs[p] = strings.Repeat(string(rune(int(base)+i)), 22)
	}
	if c.MetadataPath != "" {
		o.DirectoryIDs[c.MetadataPath] = strings.Repeat("k", 22)
		o.MetadataDirectoryID = strings.Repeat("k", 22)
	} else {
		o.MetadataDirectoryID = o.DirectoryIDs[c.DataPaths[0]]
	}
	if f.partialFormat {
		o.StorageState = "PARTIAL"
	}
	f.observations[c.Role] = o
	if f.loseFormat {
		f.loseFormat = false
		return fmt.Errorf("lost formatter response")
	}
	return nil
}
func TestCandidateCannotResumeUsingCopiedOrIncompleteFormattedIdentities(t *testing.T) {
	for _, change := range []func(*CandidateObservation){func(o *CandidateObservation) { o.MetadataDirectoryID = "" }, func(o *CandidateObservation) { delete(o.DirectoryIDs, "/data/new-controller/data") }, func(o *CandidateObservation) { o.DirectoryIDs["/data/new-controller/data"] = strings.Repeat("d", 22) }, func(o *CandidateObservation) { o.DirectoryIDs["/data/new-controller/data"] = o.MetadataDirectoryID }} {
		p := fixture()
		f := newCandidateFixture()
		f.loseFormat = true
		prepared(t, p, "controller", f)
		if _, err := runCandidate(p, "controller", "FORMAT", nil, f); err == nil {
			t.Fatal("lost result ignored")
		}
		o := f.observation("controller")
		change(&o)
		f.observations["controller"] = o
		if _, err := runCandidate(p, "controller", "FORMAT", nil, f); err == nil || f.formats != 1 {
			t.Fatal("copied/incomplete storage accepted or formatter repeated")
		}
	}
}
func (f *candidateFixture) Start(_ context.Context, _ Plan, c Process) error {
	f.starts++
	o := f.observation(c.Role)
	o.ServiceActive = true
	o.RuntimeVerified = true
	f.observations[c.Role] = o
	if f.loseStart {
		f.loseStart = false
		return fmt.Errorf("lost systemd response")
	}
	return nil
}
func (f *candidateFixture) Admit(context.Context, Plan, Process) error {
	f.admissions++
	if f.loseAdmission {
		f.loseAdmission = false
		return fmt.Errorf("lost admission response")
	}
	return nil
}
func runCandidate(p Plan, role, action string, proof *ControllerProof, f *candidateFixture) (CandidateReport, error) {
	return executeCandidate(context.Background(), p, strings.Repeat("a", 64), p.CandidateHostID, role, action, proof, f)
}
func prepared(t *testing.T, p Plan, role string, f *candidateFixture) {
	t.Helper()
	if _, err := runCandidate(p, role, "PREPARE", nil, f); err != nil {
		t.Fatal(err)
	}
}
func formatted(t *testing.T, p Plan, role string, f *candidateFixture) {
	t.Helper()
	prepared(t, p, role, f)
	if _, err := runCandidate(p, role, "FORMAT", nil, f); err != nil {
		t.Fatal(err)
	}
}
func startedController(t *testing.T, p Plan, f *candidateFixture) {
	t.Helper()
	formatted(t, p, "controller", f)
	if _, err := runCandidate(p, "controller", "START", nil, f); err != nil {
		t.Fatal(err)
	}
}
func controllerProof(p Plan, f *candidateFixture) *ControllerProof {
	c, _ := p.Process("controller")
	return &ControllerProof{ObservationID: "12345678-1234-1234-1234-123456789abe", OperationID: p.OperationID, PlanDigest: strings.Repeat("a", 64), NodeID: c.NodeID, DirectoryID: f.observation("controller").MetadataDirectoryID, BaselineDigest: p.VoterDigest(), ObservedAt: time.Now(), ObserverCaughtUp: true}
}

func TestCandidateFormatLostResultRetainsIntentAndNeverFormatsTwice(t *testing.T) {
	p := fixture()
	f := newCandidateFixture()
	prepared(t, p, "controller", f)
	f.loseFormat = true
	if _, err := runCandidate(p, "controller", "FORMAT", nil, f); err == nil {
		t.Fatal("lost result hidden")
	}
	if f.receipts["controller"].Phase != "FORMAT_INTENT" {
		t.Fatal("format intent missing")
	}
	observed, err := runCandidate(p, "controller", "INSPECT", nil, f)
	if err != nil || observed.Observation.StorageState != "MATCHING" {
		t.Fatal(err)
	}
	if _, err = runCandidate(p, "controller", "FORMAT", nil, f); err != nil {
		t.Fatal(err)
	}
	if f.formats != 1 || f.receipts["controller"].Phase != "FORMATTED" {
		t.Fatal("formatter replayed")
	}
}
func TestCandidatePartialFormatRemainsBlockedWithoutCleanup(t *testing.T) {
	p := fixture()
	f := newCandidateFixture()
	prepared(t, p, "controller", f)
	f.loseFormat = true
	f.partialFormat = true
	runCandidate(p, "controller", "FORMAT", nil, f)
	if _, err := runCandidate(p, "controller", "FORMAT", nil, f); err == nil {
		t.Fatal("partial formatter replay allowed")
	}
	if f.formats != 1 {
		t.Fatal("formatter replayed")
	}
}
func TestCandidateLostStartReconcilesOwnedRunningProcess(t *testing.T) {
	p := fixture()
	f := newCandidateFixture()
	formatted(t, p, "controller", f)
	f.loseStart = true
	if _, err := runCandidate(p, "controller", "START", nil, f); err == nil {
		t.Fatal("lost result hidden")
	}
	if _, err := runCandidate(p, "controller", "START", nil, f); err != nil {
		t.Fatal(err)
	}
	if f.starts != 1 || f.receipts["controller"].Phase != "STARTED" {
		t.Fatal("duplicate controller start")
	}
}
func TestCandidateBrokerCannotStartUntilControllerVoterProof(t *testing.T) {
	p := fixture()
	f := newCandidateFixture()
	formatted(t, p, "broker", f)
	if _, err := runCandidate(p, "broker", "START", nil, f); err == nil {
		t.Fatal("broker started without controller")
	}
	startedController(t, p, f)
	proof := controllerProof(p, f)
	if _, err := runCandidate(p, "broker", "START", proof, f); err == nil {
		t.Fatal("observer mistaken for voter")
	}
	proof.VoterVerified = true
	if _, err := runCandidate(p, "broker", "START", proof, f); err != nil {
		t.Fatal(err)
	}
	if f.starts != 2 {
		t.Fatal("unexpected process start count")
	}
}
func TestCandidateAdmissionLostResultRequiresFreshReconciliation(t *testing.T) {
	p := fixture()
	f := newCandidateFixture()
	startedController(t, p, f)
	proof := controllerProof(p, f)
	f.loseAdmission = true
	if _, err := runCandidate(p, "controller", "ADMIT", proof, f); err == nil {
		t.Fatal("lost result hidden")
	}
	if _, err := runCandidate(p, "controller", "ADMIT", proof, f); err == nil {
		t.Fatal("admission replayed with stale absence proof")
	}
	if f.admissions != 1 {
		t.Fatal("admission replayed")
	}
	proof.VoterVerified = true
	if _, err := runCandidate(p, "controller", "ADMIT", proof, f); err != nil {
		t.Fatal(err)
	}
	if f.admissions != 1 {
		t.Fatal("verified voter admitted twice")
	}
}
func TestCandidateAdmissionCanRetryOnlyAfterNewAbsenceAndCatchupProof(t *testing.T) {
	p := fixture()
	f := newCandidateFixture()
	startedController(t, p, f)
	proof := controllerProof(p, f)
	f.loseAdmission = true
	runCandidate(p, "controller", "ADMIT", proof, f)
	proof.ObservationID = "12345678-1234-1234-1234-123456789abf"
	proof.ObservedAt = time.Now().Add(time.Millisecond)
	if _, err := runCandidate(p, "controller", "ADMIT", proof, f); err != nil {
		t.Fatal(err)
	}
	if f.admissions != 2 {
		t.Fatal("fresh absence proof not honored")
	}
}
func TestCandidateDirectoryIdsArePinnedAfterFormatting(t *testing.T) {
	p := fixture()
	f := newCandidateFixture()
	formatted(t, p, "controller", f)
	o := f.observation("controller")
	o.DirectoryIDs[p.Processes[0].MetadataPath] = strings.Repeat("z", 22)
	f.observations["controller"] = o
	if _, err := runCandidate(p, "controller", "START", nil, f); err == nil {
		t.Fatal("changed storage adopted")
	}
	if f.starts != 0 {
		t.Fatal("foreign storage started")
	}
}
func TestCandidateForeignReceiptAndStaticAdmissionAreRejected(t *testing.T) {
	p := fixture()
	f := newCandidateFixture()
	prepared(t, p, "controller", f)
	r := f.receipts["controller"]
	r.PlanDigest = strings.Repeat("f", 64)
	f.receipts["controller"] = r
	if _, err := runCandidate(p, "controller", "INSPECT", nil, f); err == nil {
		t.Fatal("foreign receipt adopted")
	}
	p.QuorumMode = "STATIC"
	p.Features["kraft.version"] = 0
	p.MaintenanceAcknowledged = true
	if _, err := runCandidate(p, "controller", "ADMIT", nil, newCandidateFixture()); err == nil {
		t.Fatal("static quorum dynamically admitted")
	}
}
func TestCandidateCanceledContextCannotPerformAnyHostAction(t *testing.T) {
	p := fixture()
	f := newCandidateFixture()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := executeCandidate(ctx, p, strings.Repeat("a", 64), p.CandidateHostID, "controller", "PREPARE", nil, f); err == nil {
		t.Fatal("canceled action executed")
	}
	if len(f.receipts) != 0 || f.prepares != 0 {
		t.Fatal("canceled action mutated host")
	}
}
