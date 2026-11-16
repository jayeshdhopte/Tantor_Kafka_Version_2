//go:build linux

package expansion

import "testing"

func TestControllerMountPolicyMatchesC19ForSelectedPaths(t *testing.T) {
	for _, sample := range []string{"/ xfs rw,relatime", "/data tmpfs rw", "/data ramfs rw", "/data overlay rw", "/data xfs ro,noatime", "/other xfs rw", "", "/data xfs rw\n/other xfs rw"} {
		if persistentControllerMount(sample, "/data/controller/metadata") {
			t.Fatalf("unapproved controller mount accepted: %s", sample)
		}
	}
	if !persistentControllerMount("/data xfs rw,noatime", "/data/controller/metadata") {
		t.Fatal("persistent writable mount rejected")
	}
}
