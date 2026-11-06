//go:build linux

package expansion

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// This opt-in formats only four freshly allocated temporary test directories.
// It creates no unit and never starts or changes a Kafka service.
func TestApprovedKafka43StorageCLI(t *testing.T) {
	parcel, java := os.Getenv("TANTOR_QUORUM_CLI_PARCEL"), os.Getenv("TANTOR_QUORUM_CLI_JAVA")
	if parcel == "" || java == "" {
		t.Skip("explicit isolated storage CLI scope not supplied")
	}
	t.Setenv("TMPDIR", "/var/tmp")
	ctx, cancel := context.WithTimeout(context.Background(), 110*time.Second)
	defer cancel()
	p := fixture()
	p.ParcelDir = parcel
	p.JavaHome = java
	p.RuntimeUser = "root"
	p.ArtifactSHA256 = "71a4bba687a5623a79172c8ec688fcd45497a197117bbe8fb0ef2254d83d6380"
	manifest, err := loadManifest(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(parcel, "kafka", "4.3.0")
	if err = verifyRuntimePayload(ctx, manifest, home); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"STATIC", "DYNAMIC"} {
		for _, role := range []string{"broker", "controller"} {
			t.Run(mode+"-"+role, func(t *testing.T) {
				root := t.TempDir()
				p.QuorumMode = mode
				p.MaintenanceAcknowledged = mode == "STATIC"
				p.Features["kraft.version"] = 0
				if mode == "DYNAMIC" {
					p.Features["kraft.version"] = 1
				}
				c, _ := p.Process(role)
				c.DataPaths = []string{filepath.Join(root, "data")}
				c.MetadataPath = filepath.Join(root, "metadata")
				c.LogPath = filepath.Join(root, "logs")
				c.HeapXmx = "256m"
				for _, path := range append(storagePaths(c), c.LogPath) {
					if err := os.MkdirAll(path, 0700); err != nil {
						t.Fatal(err)
					}
				}
				for i := range p.Processes {
					if p.Processes[i].Role == role {
						p.Processes[i] = c
					}
				}
				properties, err := p.Properties(role)
				if err != nil {
					t.Fatal(err)
				}
				config := filepath.Join(root, role+".properties")
				if err = os.WriteFile(config, []byte(properties), 0600); err != nil {
					t.Fatal(err)
				}
				args := append([]string{filepath.Join(home, "bin", "kafka-storage.sh")}, formatArguments(p, c, config)...)
				cmd := exec.CommandContext(ctx, "/bin/bash", args...)
				cmd.Env = []string{"PATH=/usr/bin:/bin", "JAVA_HOME=" + java, "KAFKA_HEAP_OPTS=-Xmx256m", "LOG_DIR=" + c.LogPath}
				output, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("isolated %s/%s format failed: %v: %s", mode, role, err, output)
				}
				directories := map[string]bool{}
				for _, path := range storagePaths(c) {
					raw, err := os.ReadFile(filepath.Join(path, "meta.properties"))
					if err != nil {
						t.Fatal(err)
					}
					if !strings.Contains(string(raw), "cluster.id="+p.KafkaClusterID) || !strings.Contains(string(raw), "node.id=") {
						t.Fatal("formatted identity mismatch")
					}
					var directory string
					for _, line := range strings.Split(string(raw), "\n") {
						if strings.HasPrefix(line, "directory.id=") {
							directory = strings.TrimPrefix(line, "directory.id=")
						}
					}
					if !kafkaID.MatchString(directory) || directories[directory] {
						t.Fatal("formatted directory identity is absent or reused")
					}
					directories[directory] = true
				}
				t.Logf("Kafka 4.3.0 accepted %s/%s formatter arguments; %d fresh directory identities verified", mode, role, len(directories))
			})
		}
	}
}
