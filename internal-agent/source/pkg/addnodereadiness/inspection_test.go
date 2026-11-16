package addnodereadiness

import (
	"context"
	"testing"
)

func TestPublicFacadeRejectsMutationAndUnboundRequests(t *testing.T) {
	for _, command := range []string{"FORMAT", "START", "RESTART_SERVICE", "PATCH_KAFKA_CONFIG", "", "INSPECT", "PROBE"} {
		if _, err := Run(context.Background(), command, `{"hostId":"some-other-host"}`, "candidate"); err == nil {
			t.Fatalf("accepted %s", command)
		}
	}
}
