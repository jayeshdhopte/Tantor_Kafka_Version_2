package processinspect

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

type observedExecutor struct {
	calls   int
	changed bool
}

func (e *observedExecutor) Run(_ context.Context, command string, args ...string) (string, string, error) {
	if command != "systemctl" || len(args) != 4 || args[0] != "show" || args[1] != "tantor-kafka-broker-1.service" || args[2] != "--no-pager" {
		return "", "", fmt.Errorf("unexpected or mutating command")
	}
	e.calls++
	pid := "42"
	if e.changed && e.calls > 1 {
		pid = "43"
	}
	return "Id=tantor-kafka-broker-1.service\nLoadState=loaded\nActiveState=active\nSubState=running\nMainPID=" + pid + "\nFragmentPath=/etc/systemd/system/tantor-kafka-broker-1.service\nExecMainStartTimestampMonotonic=123456\n", "", nil
}
func (e *observedExecutor) RunSudo(context.Context, string, ...string) (string, string, error) {
	panic("inspection must never mutate/escalate")
}
func fixture() (Request, map[string][]byte) {
	r := Request{SchemaVersion: "add-node-process/v1", InspectionID: "12345678-1234-1234-1234-123456789abc", HostID: "fixture-host", ContextHash: strings.Repeat("a", 64), KafkaClusterID: strings.Repeat("a", 22), KafkaVersion: "4.3.0", NodeID: 1, Role: "broker", ServiceName: "tantor-kafka-broker-1.service", ConfigPath: "/opt/kafka/config/server.properties", StartedAt: time.Now().Add(-time.Second), ExpiresAt: time.Now().Add(120 * time.Second)}
	fields := strings.Fields("S 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 987654")
	f := map[string][]byte{r.ConfigPath: []byte("node.id=1\nprocess.roles=broker\nlog.dirs=/data/kafka/broker\nsasl.jaas.config=do-not-leak\n"), "/data/kafka/broker/meta.properties": []byte("cluster.id=" + r.KafkaClusterID + "\nnode.id=1\ndirectory.id=" + strings.Repeat("b", 22) + "\n"), "/etc/systemd/system/tantor-kafka-broker-1.service": []byte("[Service]\nExecStart=/opt/kafka/bin/kafka-server-start.sh " + r.ConfigPath + "\nEnvironment=PASSWORD=do-not-leak\n"), "/proc/42/cmdline": []byte("/usr/bin/java\x00-Dpassword=do-not-leak\x00kafka.Kafka\x00" + r.ConfigPath + "\x00"), "/proc/42/stat": []byte("42 (java worker) " + strings.Join(fields, " "))}
	return r, f
}
func fromFiles(files map[string][]byte) reader {
	return func(p string, limit int64) ([]byte, error) {
		b, ok := files[p]
		if !ok || int64(len(b)) > limit {
			return nil, fmt.Errorf("unavailable")
		}
		return b, nil
	}
}
func has(r Report, code string) bool {
	for _, c := range r.Blockers {
		if c == code {
			return true
		}
	}
	return false
}

func TestValidateBindsFreshInspectionToExactHostAndReviewedPaths(t *testing.T) {
	r, _ := fixture()
	raw, _ := json.Marshal(r)
	if _, err := Validate(string(raw), r.HostID); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Request){func(v *Request) { v.HostID = "other" }, func(v *Request) { v.ConfigPath = "/etc/shadow" }, func(v *Request) { v.ConfigPath = "/opt/kafka/../config/server.properties" }, func(v *Request) { v.ServiceName = "--all" }, func(v *Request) { v.ExpiresAt = time.Now().Add(-time.Second) }, func(v *Request) { v.ExpiresAt = v.StartedAt.Add(time.Hour) }, func(v *Request) { v.ContextHash = "unmatched" }} {
		copy := r
		change(&copy)
		raw, _ := json.Marshal(copy)
		if _, err := Validate(string(raw), r.HostID); err == nil {
			t.Fatal("unsafe inspection accepted")
		}
	}
}
func TestIdentityObservationNeverLeaksRawConfigUnitOrJVMArguments(t *testing.T) {
	r, f := fixture()
	e := &observedExecutor{}
	report := inspect(context.Background(), r, e, fromFiles(f))
	if !report.IdentityVerified || report.ExecutionReady || report.VersionVerified || e.calls != 2 || len(report.Metadata) != 1 || !has(report, "VERSION_NOT_VERIFIED") {
		t.Fatalf("incomplete report: %+v", report)
	}
	encoded, _ := json.Marshal(report)
	if strings.Contains(string(encoded), "do-not-leak") || strings.Contains(string(encoded), "PASSWORD") || strings.Contains(string(encoded), "sasl") {
		t.Fatal("raw secret-bearing content returned")
	}
}
func TestMissingForeignAndDuplicateStorageIdentityRemainBlocked(t *testing.T) {
	for _, scenario := range []string{"missing", "foreign", "duplicate"} {
		t.Run(scenario, func(t *testing.T) {
			r, f := fixture()
			switch scenario {
			case "missing":
				delete(f, "/data/kafka/broker/meta.properties")
			case "foreign":
				f["/data/kafka/broker/meta.properties"] = []byte("cluster.id=foreign\nnode.id=1\n")
			case "duplicate":
				f[r.ConfigPath] = []byte("node.id=1\nprocess.roles=broker\nlog.dirs=/data/kafka/broker,/data/kafka/second\n")
				f["/data/kafka/second/meta.properties"] = f["/data/kafka/broker/meta.properties"]
			}
			report := inspect(context.Background(), r, &observedExecutor{}, fromFiles(f))
			if report.IdentityVerified || !has(report, "STORAGE_IDENTITY_MISMATCH") {
				t.Fatal("conflicting metadata accepted")
			}
		})
	}
}
func TestPIDChangeAndConfigChangeInvalidateObservation(t *testing.T) {
	r, f := fixture()
	report := inspect(context.Background(), r, &observedExecutor{changed: true}, fromFiles(f))
	if report.IdentityVerified || !has(report, "STALE_OBSERVATION") {
		t.Fatal("changed process accepted")
	}
	reads := 0
	base := fromFiles(f)
	report = inspect(context.Background(), r, &observedExecutor{}, func(p string, limit int64) ([]byte, error) {
		if p == r.ConfigPath {
			reads++
			if reads > 1 {
				return []byte("node.id=2\n"), nil
			}
		}
		return base(p, limit)
	})
	if report.IdentityVerified || !has(report, "STALE_OBSERVATION") {
		t.Fatal("changed config accepted")
	}
}
func TestEscapedDuplicateOrWrongRolePropertiesAreNeverTrusted(t *testing.T) {
	for _, extra := range []string{"node.id=2\n", "node.\\u0069d=2\n", "process.roles=controller\n"} {
		r, f := fixture()
		f[r.ConfigPath] = append(f[r.ConfigPath], []byte(extra)...)
		report := inspect(context.Background(), r, &observedExecutor{}, fromFiles(f))
		if report.IdentityVerified || !has(report, "CONFIG_IDENTITY_MISMATCH") {
			t.Fatal("ambiguous identity accepted")
		}
	}
}
func TestUnrelatedJavaProcessCannotImpersonateKafkaUnit(t *testing.T) {
	r, f := fixture()
	f["/proc/42/cmdline"] = []byte("/usr/bin/java\x00example.Main\x00" + r.ConfigPath)
	report := inspect(context.Background(), r, &observedExecutor{}, fromFiles(f))
	if report.IdentityVerified || !has(report, "PROCESS_IDENTITY_MISMATCH") {
		t.Fatal("unrelated JVM accepted")
	}
}
func TestCombinedExistingProcessAndNodeZeroAreInspectable(t *testing.T) {
	r, f := fixture()
	r.Role = "broker,controller"
	r.NodeID = 0
	f[r.ConfigPath] = []byte("node.id=0\nprocess.roles=controller,broker\nlog.dirs=/data/kafka/broker\n")
	f["/data/kafka/broker/meta.properties"] = []byte("cluster.id=" + r.KafkaClusterID + "\nnode.id=0\ndirectory.id=" + strings.Repeat("b", 22) + "\n")
	raw, _ := json.Marshal(r)
	if _, err := Validate(string(raw), r.HostID); err != nil {
		t.Fatal(err)
	}
	if !inspect(context.Background(), r, &observedExecutor{}, fromFiles(f)).IdentityVerified {
		t.Fatal("valid existing combined process blocked")
	}
}
func TestUnreadableOrSymlinkPathsNeverTriggerRemediation(t *testing.T) {
	r, _ := fixture()
	e := &observedExecutor{}
	report := inspect(context.Background(), r, e, func(string, int64) ([]byte, error) { return nil, fmt.Errorf("symlink") })
	if report.IdentityVerified || report.ExecutionReady || len(report.Blockers) < 3 || e.calls != 2 {
		t.Fatal("unreadable files accepted or inspection did not remain bounded")
	}
}

func TestMetadataOrUnitChangeInvalidatesTheWholeObservation(t *testing.T) {
	for _, target := range []string{"/data/kafka/broker/meta.properties", "/etc/systemd/system/tantor-kafka-broker-1.service"} {
		r, files := fixture()
		base, reads := fromFiles(files), 0
		report := inspect(context.Background(), r, &observedExecutor{}, func(p string, limit int64) ([]byte, error) {
			if p == target {
				reads++
				if reads > 1 {
					return []byte("changed while inspecting"), nil
				}
			}
			return base(p, limit)
		})
		if report.IdentityVerified || !has(report, "STALE_OBSERVATION") {
			t.Fatal("changed runtime file accepted")
		}
	}
}
