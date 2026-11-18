package deploy

import (
	"os"
	"path/filepath"
	"testing"
)

func TestJavaUpgradeVersionPolicy(t *testing.T) {
	for _, tc := range []struct {
		old, next string
		ok        bool
	}{
		{"17.0.10", "17.0.21", true},
		{"17.0.21", "21.0.13", true},
		{"21.0.13", "25.0.5", true},
		{"17.0.21", "25.0.5", false},
		{"21.0.13", "17.0.21", false},
		{"21.0.13", "21.0.13", false},
		{"21", "25.0.5", false},
	} {
		if got := javaUpgradeAllowed(tc.old, tc.next); got != tc.ok {
			t.Errorf("%s to %s: got %t", tc.old, tc.next, got)
		}
	}
}

func TestSuppliedJDKArchives(t *testing.T) {
	dir := os.Getenv("TANTOR_JDK_TEST_DIR")
	if dir == "" {
		t.Skip("set TANTOR_JDK_TEST_DIR to the supplied JDK archive directory")
	}
	for file, version := range map[string]string{"jdk17.tar.gz": "17.0.21", "jdk21.tar.gz": "21.0.13", "jdk25.tar.gz": "25.0.5"} {
		t.Run(version, func(t *testing.T) {
			if err := inspectJavaArchive(filepath.Join(dir, file), version); err != nil {
				t.Fatal(err)
			}
			if err := inspectJavaArchive(filepath.Join(dir, file), "99.0.0"); err == nil {
				t.Fatal("archive accepted with the wrong version")
			}
		})
	}
}
