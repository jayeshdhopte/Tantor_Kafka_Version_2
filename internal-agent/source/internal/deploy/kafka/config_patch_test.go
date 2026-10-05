package kafka

import (
	"strings"
	"testing"
)

func valuePointer(value string) *string { return &value }

func TestParseReadablePropertiesFiltersSensitiveAndRejectsDuplicates(t *testing.T) {
	content := "# ignored\nnum.io.threads=8\nlog.retention.hours:72\nsasl.jaas.config=private\nnode.id=1\n"
	values, err := parseReadableProperties(content)
	if err != nil || len(values) != 2 || values["num.io.threads"] != "8" || values["log.retention.hours"] != "72" {
		t.Fatalf("unexpected safe properties: %v, %v", values, err)
	}
	if _, err := parseReadableProperties(content + "num.io.threads=9\n"); err == nil {
		t.Fatal("duplicate active property must be rejected")
	}
}

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
