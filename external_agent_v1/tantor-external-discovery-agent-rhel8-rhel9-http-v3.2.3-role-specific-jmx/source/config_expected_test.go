package main

import "testing"

func expectedPointer(value string) *string { return &value }

func TestValidateExpectedChangesRejectsConcurrentEdit(t *testing.T) {
	err := validateExpectedChanges(map[string]string{"num.network.threads": "4"},
		map[string]string{"num.network.threads": "5"},
		map[string]*string{"num.network.threads": expectedPointer("3")})
	if err == nil {
		t.Fatal("stale config must be rejected")
	}
}

func TestValidateExpectedChangesAllowsRetry(t *testing.T) {
	err := validateExpectedChanges(map[string]string{"num.network.threads": "5"},
		map[string]string{"num.network.threads": "5"},
		map[string]*string{"num.network.threads": expectedPointer("3")})
	if err != nil {
		t.Fatalf("already-applied value should be idempotent: %v", err)
	}
}
