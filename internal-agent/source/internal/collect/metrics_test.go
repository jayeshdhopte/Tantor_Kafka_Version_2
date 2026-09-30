package collect

import "testing"

func TestIsVirtualInterfaceName(t *testing.T) {
	virtual := []string{"virbr0", "docker0", "veth1234", "cni0", "flannel.1", "podman0", "br-deadbeef"}
	for _, name := range virtual {
		if !isVirtualInterfaceName(name) {
			t.Fatalf("expected %q to be treated as virtual", name)
		}
	}
	physical := []string{"eth0", "ens192", "enp0s3", "bond0", "team0"}
	for _, name := range physical {
		if isVirtualInterfaceName(name) {
			t.Fatalf("expected %q to be treated as a regular interface", name)
		}
	}
}

func TestManagementRouteSourceIPRejectsInvalidURL(t *testing.T) {
	if got := managementRouteSourceIP("not a valid server url"); got != "" {
		t.Fatalf("expected no preferred IP for invalid URL, got %q", got)
	}
}
