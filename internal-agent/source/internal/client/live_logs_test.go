package client

import (
	"slices"
	"testing"
	"time"
)

func TestJournalArgs(t *testing.T) {
	from := time.Date(2026, 11, 16, 13, 5, 0, 0, time.FixedZone("IST", 19800))
	to := from.Add(time.Hour)
	args, err := journalArgs(journalQuery{Unit: "tantor-kafka-7.service", From: from, To: to})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"--unit=tantor-kafka-7.service", "--since=2026-11-16 07:35:00.000000 UTC",
		"--until=2026-11-16 08:35:00.000000 UTC", "--lines=200",
	} {
		if !slices.Contains(args, want) {
			t.Errorf("missing %q in %v", want, args)
		}
	}
	for _, unit := range []string{
		"*.service", "sshd.service", "kafka.service --follow", "../broker.service",
		"schema-registry.service", "kafka-connect.service", "tantor-schema-registry.service", "tantor-kafka-connect.service",
	} {
		if _, err := journalArgs(journalQuery{Unit: unit, From: from, To: to}); err == nil {
			t.Errorf("accepted unsupported unit %q", unit)
		}
	}
	for _, invalid := range []time.Time{from.Add(-time.Minute), from.Add(8 * 24 * time.Hour)} {
		if _, err := journalArgs(journalQuery{Unit: "broker.service", From: from, To: invalid}); err == nil {
			t.Errorf("accepted out-of-range date %s", invalid)
		}
	}
}
