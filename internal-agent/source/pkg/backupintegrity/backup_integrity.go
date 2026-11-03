package backupintegrity

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// VerifyBackup reads the saved bytes and checks them against the pre-write source.
// The receipt contains no configuration contents and is safe to return in task logs.
func VerifyBackup(path string, expected []byte) (string, error) {
	saved, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read backup for integrity verification: %w", err)
	}
	actualHash, expectedHash := sha256.Sum256(saved), sha256.Sum256(expected)
	if actualHash != expectedHash {
		return "", fmt.Errorf("backup SHA-256 differs from captured source; configuration mutation refused")
	}
	receipt, err := json.Marshal(struct {
		Path       string `json:"path"`
		Checksum   string `json:"checksumSha256"`
		VerifiedAt string `json:"verifiedAt"`
		Method     string `json:"method"`
	}{path, hex.EncodeToString(actualHash[:]), time.Now().UTC().Format(time.RFC3339Nano), "SOURCE_COPY_MATCH"})
	if err != nil {
		return "", err
	}
	return "TANTOR_CONFIG_BACKUP_VERIFIED " + string(receipt), nil
}
