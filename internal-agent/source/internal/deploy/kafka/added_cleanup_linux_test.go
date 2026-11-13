//go:build linux

package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/pkg/api"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type addedCleanupExecutor struct {
	commands   []string
	failReload bool
}

func (e *addedCleanupExecutor) Run(ctx context.Context, name string, args ...string) (string, string, error) {
	return e.RunSudo(ctx, name, args...)
}
func (e *addedCleanupExecutor) RunSudo(_ context.Context, name string, args ...string) (string, string, error) {
	command := name + " " + strings.Join(args, " ")
	e.commands = append(e.commands, command)
	if e.failReload && strings.Contains(command, "daemon-reload") {
		return "", "", fmt.Errorf("simulated lost daemon reload")
	}
	if strings.Contains(command, "--property=MainPID") {
		return "0\n", "", nil
	}
	return "", "", nil
}
func TestAddedCleanupOwnsBothRolesAndRecoversAfterPartialCompletion(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root receipt ownership required")
	}
	t.Setenv("TMPDIR", "/var/tmp")
	for _, variant := range []string{"add-broker", "add-expansion", "standard-broker", "standard-expansion"} {
		family := strings.Replace(variant, "standard-", "add-", 1)
		standard := strings.HasPrefix(variant, "standard-")
		t.Run(variant, func(t *testing.T) {
			root := t.TempDir()
			configs := filepath.Join(root, "config")
			units := filepath.Join(root, "units")
			state := filepath.Join(root, "state")
			parcel := filepath.Join(root, "parcel")
			cluster := "12345678-1234-1234-1234-123456789abd"
			op := "12345678-1234-1234-1234-123456789abc"
			kafkaID := "abcdefghijklmnopqrstuv"
			write := func(path, content string) {
				t.Helper()
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			write(filepath.Join(parcel, "shared.jar"), "must remain")
			roles := []string{"broker"}
			if family == "add-expansion" {
				roles = append(roles, "controller")
			}
			plan := addedCleanupPlan{SchemaVersion: "add-node-cleanup/v1", ClusterID: cluster, HostID: "host-a"}
			for index, role := range roles {
				nodeID := 3 + index
				unit := "tantor-kafka-" + role + "-" + strconv.Itoa(nodeID) + ".service"
				if standard {
					unit = role + ".service"
				}
				data := filepath.Join(root, role, "data")
				logs := filepath.Join(root, role, "logs")
				cfg := filepath.Join(configs, family, op, role+".properties")
				if standard {
					cfg = filepath.Join(configs, "custom", role+".properties")
				}
				directory := strings.Repeat("a", 21) + strconv.Itoa(index+1)
				write(filepath.Join(data, "meta.properties"), "cluster.id="+kafkaID+"\nnode.id="+strconv.Itoa(nodeID)+"\ndirectory.id="+directory+"\n")
				write(filepath.Join(data, "000000.log"), "owned data")
				write(filepath.Join(logs, "app.log"), "owned log")
				write(cfg, "node.id="+strconv.Itoa(nodeID)+"\nprocess.roles="+role+"\nlog.dirs="+data+"\n")
				header := "# Tantor Add Broker operation " + op
				receiptPath := filepath.Join(state, "broker-addition", op, "receipt.json")
				schema := "add-broker/v1"
				if family == "add-expansion" {
					header = "# Tantor quorum expansion " + op
					receiptPath = filepath.Join(state, "quorum-expansion", op, role+".json")
					schema = "add-quorum-candidate/v1"
				}
				write(filepath.Join(units, unit), header+"\n[Service]\nEnvironment=LOG_DIR="+logs+"\nExecStart=/opt/kafka/bin/kafka-server-start.sh "+cfg+"\n")
				receipt := map[string]interface{}{"schemaVersion": schema, "operationId": op, "hostId": "host-a", "planDigest": strings.Repeat("a", 64), "phase": "STARTED", "kafkaClusterId": kafkaID, "nodeId": nodeID, "role": role, "serviceName": strings.TrimSuffix(unit, ".service"), "unit": unit, "configPath": cfg}
				if family == "add-expansion" {
					receipt["directoryIds"] = map[string]string{data: directory}
				}
				body, _ := json.Marshal(receipt)
				write(receiptPath, string(body))
				plan.Processes = append(plan.Processes, addedCleanupProcess{OperationID: op, PlanDigest: strings.Repeat("a", 64), HostID: "host-a", Role: role, NodeID: nodeID, Unit: unit, ConfigPath: cfg, KafkaClusterID: kafkaID, DataPaths: []string{data}, LogPath: logs, RuntimeHome: parcel})
			}
			cfg := &config.Config{}
			cfg.Agent.HostID = "host-a"
			cfg.Paths.DataDir = state
			executor := &addedCleanupExecutor{failReload: true}
			deployer := &Deployer{cfg: cfg, exec: executor}
			raw, _ := json.Marshal(plan)
			task := &api.Task{ClusterID: cluster, Parameters: map[string]string{"add_node_cleanup": string(raw)}}
			// A foreign metadata identity must reject the whole request before either unit stops.
			meta := filepath.Join(plan.Processes[0].DataPaths[0], "meta.properties")
			original, _ := os.ReadFile(meta)
			write(meta, strings.ReplaceAll(string(original), kafkaID, strings.Repeat("z", 22)))
			if _, err := deployer.cleanAddedNodesAt(context.Background(), task, configs, units); err == nil {
				t.Fatal("foreign storage accepted")
			}
			if len(executor.commands) != 0 {
				t.Fatal("preflight failure stopped a unit")
			}
			write(meta, string(original))
			if _, err := deployer.cleanAddedNodesAt(context.Background(), task, configs, units); err == nil {
				t.Fatal("injected late failure was not propagated")
			}
			for _, process := range plan.Processes {
				if _, err := os.Stat(process.DataPaths[0]); !os.IsNotExist(err) {
					t.Fatal("owned storage was not removed")
				}
				if _, err := os.Stat(filepath.Join(units, process.Unit)); !os.IsNotExist(err) {
					t.Fatal("owned unit was not removed")
				}
			}
			executor.failReload = false
			if _, err := deployer.cleanAddedNodesAt(context.Background(), task, configs, units); err != nil {
				t.Fatal("partial completion did not resume", err)
			}
			before := len(executor.commands)
			if _, err := deployer.cleanAddedNodesAt(context.Background(), task, configs, units); err != nil {
				t.Fatal(err)
			}
			if len(executor.commands) != before {
				t.Fatal("completed cleanup repeated service actions")
			}
			if _, err := os.Stat(filepath.Join(parcel, "shared.jar")); err != nil {
				t.Fatal("shared runtime was deleted")
			}
			plan.HostID = "another-host"
			raw, _ = json.Marshal(plan)
			task.Parameters["add_node_cleanup"] = string(raw)
			if _, err := deployer.cleanAddedNodesAt(context.Background(), task, configs, units); err == nil {
				t.Fatal("foreign host accepted")
			}
		})
	}
}
