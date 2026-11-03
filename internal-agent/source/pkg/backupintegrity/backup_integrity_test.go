package backupintegrity

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBackupReceiptHashesExactSavedBytesWithoutExposingContents(t *testing.T) {
	content := []byte("# keep comments\r\npassword=private-content\r\nnode.id=1\n")
	path := filepath.Join(t.TempDir(), "broker.properties.bak")
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatal(err)
	}
	line, err := VerifyBackup(path, content)
	if err != nil {
		t.Fatal(err)
	}
	var receipt map[string]string
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "TANTOR_CONFIG_BACKUP_VERIFIED ")), &receipt); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(content)
	if receipt["checksumSha256"] != hex.EncodeToString(digest[:]) || receipt["path"] != path || receipt["method"] != "SOURCE_COPY_MATCH" {
		t.Fatal(receipt)
	}
	if strings.Contains(line, "private-content") {
		t.Fatal("receipt leaked configuration contents")
	}
	if _, err := time.Parse(time.RFC3339Nano, receipt["verifiedAt"]); err != nil {
		t.Fatal(err)
	}
}

func TestChangedOrMissingBackupCannotProduceVerificationEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backup")
	if err := os.WriteFile(path, []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if line, err := VerifyBackup(path, []byte("original")); err == nil || line != "" {
		t.Fatal("changed backup accepted")
	}
	if line, err := VerifyBackup(path+".missing", nil); err == nil || line != "" {
		t.Fatal("missing backup accepted")
	}
}
