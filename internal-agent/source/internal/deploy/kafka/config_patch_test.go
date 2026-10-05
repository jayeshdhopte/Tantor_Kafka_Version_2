package kafka

import (
	"strings"
	"testing"
)

func valuePointer(value string) *string { return &value }

func TestPatchPropertiesPreservesUnrelatedSettings(t *testing.T) {
	input := "# Kafka node\nnode.id=102\nnum.network.threads=3\nlog.dirs=/data/kafka\n"
	output, changed, err := patchProperties(input,
		map[string]*string{"num.network.threads": valuePointer("5")},
		map[string]*string{"num.network.threads": valuePointer("3")})
	if err != nil || !changed {
		t.Fatalf("patch failed: changed=%v err=%v", changed, err)
	}
	if !strings.Contains(output, "num.network.threads=5") ||
		!strings.Contains(output, "node.id=102") || !strings.Contains(output, "log.dirs=/data/kafka") {
		t.Fatalf("unrelated config was lost: %q", output)
	}
}

func TestPatchPropertiesRejectsStaleValue(t *testing.T) {
	_, _, err := patchProperties("num.network.threads=4\n",
		map[string]*string{"num.network.threads": valuePointer("5")},
		map[string]*string{"num.network.threads": valuePointer("3")})
	if err == nil {
		t.Fatal("stale preview must be rejected")
	}
}

func TestPatchPropertiesRetryAfterWrite(t *testing.T) {
	output, changed, err := patchProperties("num.network.threads=5\n",
		map[string]*string{"num.network.threads": valuePointer("5")},
		map[string]*string{"num.network.threads": valuePointer("3")})
	if err != nil || changed || output != "num.network.threads=5\n" {
		t.Fatalf("retry should be idempotent: changed=%v err=%v output=%q", changed, err, output)
	}
}
