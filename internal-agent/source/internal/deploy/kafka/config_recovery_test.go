package kafka

import "testing"

func TestRecoveryRestoresAbsenceAndPreservesUnrelatedValues(t *testing.T) {
	old, newValue := "8", "12"
	changes := map[string]*string{"num.io.threads": &old, "broker.session.timeout.ms": nil}
	expected := map[string]*string{"num.io.threads": &newValue, "broker.session.timeout.ms": &newValue}
	parameters := map[string]string{"config_recovery": "true", "checkpoint_id": "rollback-12345678-1234-1234-1234-123456789abc"}
	if err := ValidateConfigRecovery(parameters, changes, expected); err != nil {
		t.Fatal(err)
	}
	result, changed, err := ApplyPropertyPatch("node.id=1\nnum.io.threads=12\nbroker.session.timeout.ms=12\nlog.retention.hours=48\n", changes, expected)
	if err != nil || !changed {
		t.Fatalf("recovery failed: %v", err)
	}
	props, err := ReadableProperties(result)
	if err != nil || props["num.io.threads"] != "8" || props["log.retention.hours"] != "48" {
		t.Fatalf("unexpected recovery: %s (%v)", result, err)
	}
	if _, found := props["broker.session.timeout.ms"]; found {
		t.Fatal("absent property was not removed")
	}
	if _, changed, err := ApplyPropertyPatch(result, changes, expected); err != nil || changed {
		t.Fatalf("recovery retry must be idempotent: %v", err)
	}
}

func TestRecoveryRejectsDriftAndInvalidCheckpoint(t *testing.T) {
	old, desired, drift := "8", "12", "16"
	changes, expected := map[string]*string{"num.io.threads": &old}, map[string]*string{"num.io.threads": &desired}
	if _, _, err := ApplyPropertyPatch("num.io.threads="+drift+"\n", changes, expected); err == nil {
		t.Fatal("drift must stop recovery")
	}
	if err := ValidateConfigRecovery(map[string]string{"config_recovery": "true"}, changes, expected); err == nil {
		t.Fatal("missing checkpoint accepted")
	}
	p := map[string]string{"config_recovery": "true", "checkpoint_id": "rollback-12345678-1234-1234-1234-123456789abc"}
	blank := ""
	if err := ValidateConfigRecovery(p, map[string]*string{"num.io.threads": &blank}, expected); err == nil {
		t.Fatal("blank rollback accepted")
	}
	if err := ValidateConfigRecovery(p, changes, map[string]*string{}); err == nil {
		t.Fatal("missing expectations accepted")
	}
}

func TestRecoveryChecksNodeIdentityBeforeWriting(t *testing.T) {
	p := map[string]string{"config_recovery": "true", "node_id": "1"}
	if err := validateRecoveryNode(p, "node.id=1\nbroker.id=1\n"); err != nil {
		t.Fatal(err)
	}
	for _, content := range []string{"node.id=2\n", "node.id=1\nbroker.id=2\n", "log.retention.hours=48\n"} {
		if err := validateRecoveryNode(p, content); err == nil {
			t.Fatalf("accepted changed or missing node identity: %s", content)
		}
	}
}
