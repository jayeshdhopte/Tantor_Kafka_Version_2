package expansion

import (
	_ "embed"
	"testing"
)

//go:embed testdata/separate-dynamic-v1.json
var frozenWireFixture string

func TestBackendNativeSharedFrozenPlan(t *testing.T) {
	p, err := Decode(frozenWireFixture, Digest(frozenWireFixture), "candidate")
	if err != nil {
		t.Fatal(err)
	}
	if p.Processes[0].NodeID != 102 || p.Processes[1].NodeID != 2 || p.Layout != "SEPARATE" || p.KafkaVersion != "4.3.0" {
		t.Fatal("wire fixture lost process/version identity")
	}
	if Digest(frozenWireFixture+" ") == Digest(frozenWireFixture) {
		t.Fatal("raw frozen bytes lost identity")
	}
}
