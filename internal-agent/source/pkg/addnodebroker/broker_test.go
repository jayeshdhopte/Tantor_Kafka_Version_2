package addnodebroker

import (
	"context"
	"testing"
)

func TestBridgeRejectsForeignOrArbitraryWork(t *testing.T) {
	for _, command := range []string{"INSTALL_KAFKA", "RESTART_SERVICE", "FORMAT_ADD_BROKER", "START_ADD_BROKER", "VALIDATE_ADD_BROKER_PLAN", "INSPECT_ADD_BROKER"} {
		if _, err := Run(context.Background(), command, "candidate", t.TempDir(), map[string]string{}, nil); err == nil {
			t.Fatalf("accepted invalid %s", command)
		}
	}
}
