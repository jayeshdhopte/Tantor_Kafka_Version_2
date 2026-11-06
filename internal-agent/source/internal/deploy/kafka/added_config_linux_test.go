//go:build linux

package kafka

import (
	"encoding/json"
	"io.translab/tantor-agent/pkg/api"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAddedConfigRequiresOwnedCompletedIdentity(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root-owned addition receipt test")
	}
	for _, family := range []string{"add-broker", "add-expansion"} {
		t.Run(family, func(t *testing.T) {
			root := t.TempDir()
			configs := filepath.Join(root, "config")
			units := filepath.Join(root, "units")
			data := filepath.Join(root, "state")
			op := "12345678-1234-1234-1234-123456789abc"
			file := filepath.Join(configs, family, op, "broker.properties")
			receiptPath := filepath.Join(data, "broker-addition", op, "receipt.json")
			schema := "add-broker/v1"
			header := "# Tantor Add Broker operation " + op
			if family == "add-expansion" {
				receiptPath = filepath.Join(data, "quorum-expansion", op, "broker.json")
				schema = "add-quorum-candidate/v1"
				header = "# Tantor quorum expansion " + op
			}
			unit := "tantor-kafka-broker-3.service"
			write := func(path, content string) {
				t.Helper()
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			receipt := map[string]interface{}{"schemaVersion": schema, "operationId": op, "hostId": "host-a", "planDigest": strings.Repeat("a", 64), "phase": "STARTED", "kafkaClusterId": "abcdefghijklmnopqrstuv", "nodeId": 3, "role": "broker", "serviceName": "tantor-kafka-broker-3", "unit": unit, "configPath": file}
			save := func() { raw, _ := json.Marshal(receipt); write(receiptPath, string(raw)) }
			save()
			write(file, "node.id=3\nprocess.roles=broker\nnum.network.threads=4\n")
			unitPath := filepath.Join(units, unit)
			unitBody := header + "\n[Service]\nExecStart=/opt/kafka/bin/kafka-server-start.sh " + file + "\n"
			write(unitPath, unitBody)
			task := &api.Task{Parameters: map[string]string{"config_path": file, "role": "broker", "node_id": "3", "service_name": "tantor-kafka-broker-3"}}
			check := func(want bool) {
				t.Helper()
				_, err := validateAddedConfigAt(task, data, "host-a", configs, units)
				if (err == nil) != want {
					t.Fatalf("want valid=%v, got %v", want, err)
				}
			}
			check(true)
			receipt["hostId"] = "host-b"
			save()
			check(false)
			receipt["hostId"] = "host-a"
			save()
			receipt["phase"] = "FORMAT_INTENT"
			save()
			check(false)
			receipt["phase"] = "STARTED"
			save()
			task.Parameters["node_id"] = "4"
			check(false)
			task.Parameters["node_id"] = "3"
			task.Parameters["service_name"] = "broker"
			check(false)
			task.Parameters["service_name"] = "tantor-kafka-broker-3.service"
			write(file, "node.id=3\nnode.id=3\nprocess.roles=broker\n")
			check(false)
			write(file, "node.id=3\nprocess.roles=broker\nnum.network.threads=8\n")
			check(true)
			write(unitPath, strings.ReplaceAll(unitBody, file, "/opt/other/broker.properties"))
			check(false)
			write(unitPath, unitBody)
			if err := os.Chmod(receiptPath, 0666); err != nil {
				t.Fatal(err)
			}
			check(false)
			os.Chmod(receiptPath, 0600)
			moved := file + ".original"
			if err := os.Rename(file, moved); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(moved, file); err != nil {
				t.Fatal(err)
			}
			check(false)
		})
	}
}
