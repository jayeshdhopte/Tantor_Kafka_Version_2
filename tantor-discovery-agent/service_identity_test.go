package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSystemdIdentityDuringProcessStartup(t *testing.T) {
	for _, value := range []string{"0::/system.slice/broker.service", "1:name=systemd:/system.slice/broker.service"} {
		if got := systemdUnitFromCgroup(value); got != "broker.service" {
			t.Fatalf("unit %q", got)
		}
	}
	if got := systemdUnitFromCgroup("0::/user.slice/user-1000.slice/session-1.scope"); got != "" {
		t.Fatal(got)
	}
}

func TestDuplicateProcessDiscoveryKeepsServiceIdentityInBothOrders(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "broker.properties")
	alias := filepath.Join(dir, "alias.properties")
	if err := os.WriteFile(real, []byte("node.id=1"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, alias); err != nil {
		t.Fatal(err)
	}
	proc := ProcessInfo{PropsFile: real, Pid: "123"}
	unit := ProcessInfo{PropsFile: alias, SystemdUnit: "broker.service"}
	for _, records := range [][]ProcessInfo{{proc, unit}, {unit, proc}} {
		got := processInfoByConfig(records)[real]
		if got.SystemdUnit != "broker.service" || got.Pid != "123" {
			t.Fatalf("lost identity: %+v", got)
		}
	}
}
