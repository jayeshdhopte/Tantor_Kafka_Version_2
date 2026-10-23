//go:build !linux

package artifactwork

import "os"

// Release agents are Linux binaries; these stubs support offline source tooling.
func lockFile(file *os.File) error   { return nil }
func unlockFile(file *os.File) error { return nil }
