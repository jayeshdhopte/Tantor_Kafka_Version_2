package kafka

import "os"

// Production agents run on Linux. This keeps the pure patch parser testable on Windows.
func preservePatchOwner(_ *os.File, _ os.FileInfo) error { return nil }
